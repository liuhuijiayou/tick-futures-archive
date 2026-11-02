"""Private, owner-verified Release assets; content-addressed immutable uploads.

The GitHub connector has no Release-upload operation. Use authenticated gh, with
no token arguments, no LFS, no overwrite and no automatic data authorization.
"""

import json
from pathlib import Path
import subprocess
import tempfile
import time
from urllib.parse import quote

from .archive import SAFE_ASSETS_PER_RELEASE, decode, file_hash, restore, restore_dataset, validate_manifest
from .codec import sha256
from .runtime import atomic_json


def command(args, *, output=None):
    last = None
    for attempt in range(3):
        try:
            if output:
                with Path(output).open("wb") as stream:
                    subprocess.run(["gh", *args], stdout=stream, stderr=subprocess.PIPE,
                                   check=True, timeout=180)
                return None
            return subprocess.check_output(["gh", *args], stderr=subprocess.PIPE, timeout=180)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            last = exc
            if isinstance(exc, subprocess.CalledProcessError) and b"404" in (exc.stderr or b""):
                raise
            if attempt < 2:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"GitHub operation failed after 3 attempts: {args[:3]}") from last


def api(path):
    return json.loads(command(["api", path]))


def verify_owner_private(repo, *, require_push=True):
    owner = command(["api", "user", "--jq", ".login"]).decode().strip()
    info = api("repos/" + repo)
    if not info["private"] or info["owner"]["login"].lower() != owner.lower() or info["owner"]["type"] != "User":
        raise ValueError("Expected a private repository owned by the authenticated personal account")
    if require_push and not info.get("permissions", {}).get("push"):
        raise PermissionError("Repository push permission is required")
    return info


def release_info(repo, tag):
    try:
        return api(f"repos/{repo}/releases/tags/{quote(tag, safe='')}")
    except subprocess.CalledProcessError as exc:
        if b"404" not in (exc.stderr or b""):
            raise
        # An unpublished draft may have no Git tag yet. The by-tag endpoint
        # then returns 404 even to its owner; list drafts to resume safely.
        pages=json.loads(command(["api","--paginate","--slurp",f"repos/{repo}/releases?per_page=100"]))
        matches=[r for page in pages for r in page if r["tag_name"]==tag]
        if len(matches)==1:
            return matches[0]
        if len(matches)>1:
            raise ValueError("Ambiguous duplicate Release tags")
        raise


def asset_list(repo, release):
    pages = json.loads(command(["api", "--paginate", "--slurp", f"repos/{repo}/releases/{release['id']}/assets?per_page=100"]))
    return [asset for page in pages for asset in page]


def create_release(repo,tag,title,body):
    # Use the create response's immutable ID immediately. Draft visibility in
    # by-tag/list queries can lag creation, so do not rediscover a new draft.
    return json.loads(command(["api",f"repos/{repo}/releases","--method","POST",
        "--raw-field","tag_name="+tag,"--raw-field","name="+title,
        "--raw-field","body="+body,"--field","draft=true"]))


def download(repo, asset, target):
    command(["api", f"repos/{repo}/releases/assets/{asset['id']}",
             "-H", "Accept: application/octet-stream"], output=target)


def find_archive_tag(repo,archive_id):
    pages=json.loads(command(["api","--paginate","--slurp",f"repos/{repo}/releases?per_page=100"]))
    name=f"complete-{archive_id}.json"
    matches=[r["tag_name"] for page in pages for r in page if any(a["name"]==name for a in r["assets"])]
    if len(matches)!=1:
        raise ValueError("Missing or ambiguous completed parent archive: "+archive_id)
    return matches[0]


def upload(archive, repo, tag, *, authorization=None, fault_after_parts=None):
    archive = Path(archive)
    manifest = json.loads((archive / "manifest.json").read_text())
    names = validate_manifest(manifest)
    verify_owner_private(repo)
    if manifest["data_kind"] != "synthetic":
        auth = json.loads(Path(authorization).read_text()) if authorization else {}
        if not (auth.get("status") == "approved" and auth.get("private_backup") is True
                and auth.get("source") == manifest["source"] and auth.get("evidence_url") and auth.get("checked_at")):
            raise PermissionError("Market-data private off-host backup rights are PENDING; no market assets uploaded")
    parent_tag=find_archive_tag(repo,manifest["parent"]) if manifest.get("parent") else None
    try:
        release = release_info(repo, tag)
    except subprocess.CalledProcessError as exc:
        if b"404" not in (exc.stderr or b""):
            raise
        release = create_release(repo,tag,tag,"Immutable archive. Collection integrity is specified separately in its manifest.")
    assets = {a["name"]: a for a in asset_list(repo, release)}
    manifest_name = f"manifest-{manifest['archive_id']}.json"
    completion_name = f"complete-{manifest['archive_id']}.json"
    planned = set(names + [manifest_name, completion_name])
    if len(set(assets) | planned) > SAFE_ASSETS_PER_RELEASE:
        raise ValueError("Release safety count exceeded; use a new date/market/group release")
    remote_records = []
    uploaded = 0
    with tempfile.TemporaryDirectory(prefix="tick-upload-verify-") as folder:
        temp = Path(folder)
        for entry in manifest["files"]:
            for part in entry["parts"]:
                name = part["name"]
                source = archive / name
                if source.stat().st_size != part["stored_bytes"] or file_hash(source) != part["stored_sha256"]:
                    raise ValueError(f"Local shard is corrupt: {name}")
                if name not in assets:
                    command(["release", "upload", tag, str(source), "--repo", repo])
                    uploaded += 1
                    assets = {a["name"]: a for a in asset_list(repo, release)}
                # Existing names are downloaded too: size/name is never proof.
                target = temp / name
                download(repo, assets[name], target)
                if target.stat().st_size != part["stored_bytes"] or file_hash(target) != part["stored_sha256"]:
                    raise ValueError(f"Remote same-name/different-content conflict: {name}; never overwritten")
                raw = decode(target.read_bytes(), part["codec"], expected_size=part["raw_bytes"])
                if len(raw) != part["raw_bytes"] or sha256(raw) != part["raw_sha256"]:
                    raise ValueError(f"Remote decode verification failed: {name}")
                remote_records.append({"name": name, "id": assets[name]["id"], "size": assets[name]["size"],
                                       "sha256": part["stored_sha256"], "download_decode_verified": True})
                if fault_after_parts and len(remote_records)>=fault_after_parts:
                    raise RuntimeError("Injected interruption after verified data asset; completion not published")
        def ensure_metadata(name, source):
            nonlocal assets, uploaded
            if name not in assets:
                staged = temp / name
                staged.write_bytes(source.read_bytes())
                command(["release", "upload", tag, str(staged), "--repo", repo])
                uploaded += 1
                assets = {a["name"]: a for a in asset_list(repo, release)}
            target = temp / (name + ".verify")
            download(repo, assets[name], target)
            if target.read_bytes() != source.read_bytes():
                raise ValueError(f"Remote immutable metadata conflict: {name}")
        ensure_metadata(manifest_name, archive / "manifest.json")
        completion = {"format": "tfcomplete-1", "archive_id": manifest["archive_id"], "repo": repo, "tag": tag,
                      "manifest_name": manifest_name, "manifest_sha256": file_hash(archive / "manifest.json"),
                      "assets": remote_records, "collection_integrity": manifest["collection_integrity"],
                      "archive_integrity": "all_assets_downloaded_and_decoded_verified"}
        if parent_tag:
            completion["parent_tag"]=parent_tag
        local_completion = temp / "receipt.json"
        atomic_json(local_completion, completion)
        ensure_metadata(completion_name, local_completion)  # completion is published LAST
        if release.get("draft"):
            command(["release", "edit", tag, "--repo", repo, "--draft=false"])
    receipt = {**completion, "uploaded_this_attempt": uploaded, "downloaded_decoded_parts": len(remote_records),
               "release_url": f"https://github.com/{repo}/releases/tag/{quote(tag,safe='')}"}
    atomic_json(archive / "remote-receipt.json", receipt)
    return receipt


def restore_github(repo, tag, output, *, archive_id=None):
    verify_owner_private(repo,require_push=False)
    with tempfile.TemporaryDirectory(prefix="tick-github-only-") as folder:
        cache = Path(folder)
        downloaded=0
        fetched={}
        def fetch(current_tag,wanted=None):
            nonlocal downloaded
            release=release_info(repo,current_tag)
            assets={a["name"]:a for a in asset_list(repo,release)}
            complete_names=[n for n in assets if n.startswith("complete-") and n.endswith(".json") and (wanted is None or n==f"complete-{wanted}.json")]
            if len(complete_names)!=1:
                raise ValueError("Select exactly one completed archive; incomplete/missing batches cannot restore")
            target=cache/(wanted or "leaf")
            target.mkdir()
            download(repo,assets[complete_names[0]],target/"completion.json")
            completion=json.loads((target/"completion.json").read_text())
            if completion.get("format")!="tfcomplete-1":
                raise ValueError("Incompatible completion version")
            name=completion["manifest_name"]
            if name not in assets:
                raise ValueError("Missing remote manifest")
            download(repo,assets[name],target/"manifest.json")
            if file_hash(target/"manifest.json")!=completion["manifest_sha256"]:
                raise ValueError("Remote manifest hash mismatch")
            manifest=json.loads((target/"manifest.json").read_text())
            key=manifest["archive_id"]
            if key!=completion["archive_id"] or (wanted is not None and key!=wanted):
                raise ValueError("Wrong remote archive identity")
            if key in fetched:
                raise ValueError("Archive parent cycle")
            fetched[key]=target
            names=validate_manifest(manifest)
            if [p["name"] for p in completion["assets"]]!=names:
                raise ValueError("Completion asset order/set does not match manifest")
            for name in names:
                if name not in assets:
                    raise ValueError("Missing remote shard: "+name)
                download(repo,assets[name],target/name)
            downloaded+=len(names)+2
            parent=manifest.get("parent")
            if parent:
                fetch(completion.get("parent_tag") or find_archive_tag(repo,parent),parent)
            return key
        leaf=fetch(tag,archive_id)
        if len(fetched)==1:
            report=restore(fetched[leaf],output)
        else:
            report=restore_dataset(cache,output,leaf=leaf)
        report.update({"remote_restore": True, "repo": repo, "tag": tag,
                       "downloaded_assets": downloaded, "production_data_or_cache_used": False})
        atomic_json(Path(output) / "restore-report.json", report)
        return report
