import asyncio
import html
from dataclasses import dataclass, field

import discord

import state
from config import (
    MAX_APWORLD_BYTES, MAX_APWORLD_FILES, MAX_GENERATION_MEMORY,
    MAX_YAML_BYTES, MAX_YAML_FILES, MSG_MEMORY_FULL, GITHUB_RELEASE_RE,
)
from utils.files import apworld_stem, normalise_yaml_bytes, safe_filename
from utils.github import download_apworld_from_github
from utils.versions import _norm, get_installed_versions, get_version_dir, parse_version
from utils.yaml_validation import (
    check_yamls_on_server, get_apworld_info, get_builtin_game_names,
    get_min_ap_version, get_yaml_game, get_yaml_requires,
)


@dataclass
class ScanResult:
    yaml_data:         dict = field(default_factory=dict)
    apworld_data:      dict = field(default_factory=dict)
    yaml_uploaders:    dict = field(default_factory=dict)
    apworld_uploaders: dict = field(default_factory=dict)
    reserved_bytes:    int  = 0
    had_error:         bool = False
    issues:            list = field(default_factory=list)


async def handle_github_link(
    match, thread, author, message_id: int, seen_stems: set, seen_repos: dict,
    audit: bool = False, issues: list | None = None,
) -> tuple[str, bytes] | None | bool:
    owner, repo, tag = match.group(1), match.group(2), match.group(3)
    repo_key  = f"{owner}/{repo}".lower()
    tag_lower = tag.lower()
    mention   = author.mention
    link      = match.group(0)

    if repo_key in seen_repos:
        if seen_repos[repo_key] == tag_lower:
            return None
        msg = (
            f"{mention} ⚠️ Multiple releases linked for **{owner}/{repo}**: "
            f"`{seen_repos[repo_key]}` and `{tag}` — remove one."
        )
        if audit:
            if issues is not None:
                issues.append((f"multi_release:{message_id}", msg))
            return None
        await thread.send(msg)
        return False

    seen_repos[repo_key] = tag_lower

    try:
        loop = asyncio.get_running_loop()
        filename, data = await loop.run_in_executor(
            None, download_apworld_from_github, owner, repo, tag
        )
    except Exception as e:
        msg = f"{mention} ⚠️ **{link}**: could not download apworld — `{e}`"
        if audit:
            if issues is not None:
                issues.append((f"download_failed:{message_id}", msg))
            return None
        await thread.send(msg)
        return None

    stem = apworld_stem(filename)
    if stem in seen_stems:
        msg = f"{mention} ⚠️ **{stem}**: duplicate apworld — GitHub link conflicts with an already posted file."
        if audit:
            if issues is not None:
                issues.append((f"dup_apworld_github:{message_id}:{stem}", msg))
            return None
        await thread.send(msg)
        return False

    seen_stems.add(stem)
    return filename, data


async def collect_files_from_thread(
    thread,
    bot_user: discord.User,
    audit: bool = False,
) -> ScanResult:
    result             = ScanResult()
    seen_apworld_stems = set()
    seen_repos: dict   = {}

    def abort() -> ScanResult:
        state.memory_in_use   -= result.reserved_bytes
        result.reserved_bytes  = 0
        result.had_error       = True
        return result

    async for message in thread.history(limit=500, oldest_first=True):
        if message.author == bot_user:
            continue
        mention = message.author.mention

        for attachment in message.attachments:
            name      = attachment.filename.lower()
            safe_name = safe_filename(attachment.filename)

            if name.endswith(".yaml") or name.endswith(".yml"):
                if len(result.yaml_data) >= MAX_YAML_FILES:
                    if not audit:
                        await thread.send(f"⚠️ Too many YAML files (max {MAX_YAML_FILES}). Only the first {MAX_YAML_FILES} will be used.")
                    return result

                if attachment.size > MAX_YAML_BYTES:
                    msg = f"{mention} ⚠️ **{safe_name}**: too large ({attachment.size // 1024} KB, max {MAX_YAML_BYTES // 1024} KB)."
                    if audit:
                        result.issues.append((f"{message.id}:yaml_too_large:{safe_name}", msg))
                        continue
                    await thread.send(msg)
                    return abort()

                if state.memory_in_use + attachment.size > MAX_GENERATION_MEMORY:
                    if audit:
                        result.issues.append(("memory_full", MSG_MEMORY_FULL))
                        return result
                    await thread.send(MSG_MEMORY_FULL)
                    return abort()

                state.memory_in_use   += attachment.size
                result.reserved_bytes += attachment.size
                result.yaml_data[safe_name]      = normalise_yaml_bytes(await attachment.read())
                result.yaml_uploaders[safe_name] = message.author

            elif name.endswith(".apworld"):
                if len(result.apworld_data) >= MAX_APWORLD_FILES:
                    if not audit:
                        await thread.send(f"⚠️ Too many apworld files (max {MAX_APWORLD_FILES}).")
                        return abort()
                    continue

                if attachment.size > MAX_APWORLD_BYTES:
                    msg = f"{mention} ⚠️ **{safe_name}**: too large ({attachment.size // 1024 // 1024} MB, max {MAX_APWORLD_BYTES // 1024 // 1024} MB)."
                    if audit:
                        result.issues.append((f"{message.id}:apworld_too_large:{safe_name}", msg))
                        continue
                    await thread.send(msg)
                    return abort()

                if state.memory_in_use + attachment.size > MAX_GENERATION_MEMORY:
                    if audit:
                        result.issues.append(("memory_full", MSG_MEMORY_FULL))
                        return result
                    await thread.send(MSG_MEMORY_FULL)
                    return abort()

                stem = apworld_stem(attachment.filename)
                if stem in seen_apworld_stems:
                    msg = f"{mention} ⚠️ **{safe_name}**: duplicate apworld — remove one."
                    if audit:
                        result.issues.append((f"{message.id}:dup_apworld:{stem}", msg))
                        continue
                    await thread.send(msg)
                    return abort()

                state.memory_in_use   += attachment.size
                result.reserved_bytes += attachment.size
                result.apworld_data[safe_name]      = await attachment.read()
                result.apworld_uploaders[safe_name] = message.author
                seen_apworld_stems.add(stem)

        for match in GITHUB_RELEASE_RE.finditer(message.content or ""):
            gh = await handle_github_link(
                match, thread, message.author, message.id, seen_apworld_stems, seen_repos,
                audit=audit, issues=result.issues,
            )
            if gh is False:
                if audit:
                    continue
                return abort()
            if gh is not None:
                filename, data = gh
                file_size = len(data)
                if state.memory_in_use + file_size > MAX_GENERATION_MEMORY:
                    if audit:
                        result.issues.append(("memory_full", MSG_MEMORY_FULL))
                        return result
                    await thread.send(MSG_MEMORY_FULL)
                    return abort()
                state.memory_in_use   += file_size
                result.reserved_bytes += file_size
                result.apworld_data[filename]      = data
                result.apworld_uploaders[filename] = message.author

    return result


async def audit_thread(thread, bot_user: discord.User) -> ScanResult:
    result = await collect_files_from_thread(thread, bot_user, audit=True)

    versions      = get_installed_versions()
    builtin_games = get_builtin_game_names(get_version_dir(versions[0])) if versions else set()

    apworld_infos = {name: get_apworld_info(data) for name, data in result.apworld_data.items()}

    min_ap_ver = get_min_ap_version(result.yaml_data, apworld_infos=apworld_infos)
    if min_ap_ver:
        min_ap_parsed = parse_version(min_ap_ver)
        satisfying = [v for v in versions if parse_version(v) >= min_ap_parsed]
        if not satisfying:
            latest = versions[0] if versions else "none"
            result.issues.append((
                f"ap_version_too_old:{min_ap_ver}",
                f"⚠️ Your YAMLs require Archipelago `{min_ap_ver}` or newer, "
                f"but the latest installed version is `{latest}`.",
            ))

    # Prefer the game name extracted from the apworld's Python source over the filename stem,
    # so that abbreviations like "dkc.apworld" match a YAML whose game is "Donkey Kong Country".
    apworld_keys_norm = {
        _norm(apworld_infos[name]["game"]) if apworld_infos[name].get("game") else _norm(apworld_stem(name)): name
        for name in result.apworld_data
    }
    yaml_games_by_name = {name: get_yaml_game(data) for name, data in result.yaml_data.items()}

    yaml_games_normalised = {_norm(game or "") for game in yaml_games_by_name.values()}
    for norm_key, apworld_name in apworld_keys_norm.items():
        has_yaml = any(
            norm_key in game_norm or game_norm in norm_key
            for game_norm in yaml_games_normalised
            if game_norm
        )
        if not has_yaml:
            uploader = result.apworld_uploaders.get(apworld_name)
            mention  = uploader.mention if uploader else ""
            result.issues.append((
                f"missing_yaml:{apworld_name}",
                f"{mention} ⚠️ **{apworld_name}**: apworld provided but no matching YAML found — please post a YAML for this game.",
            ))

    if result.yaml_data:
        yamls_to_validate = {}
        for name, data in result.yaml_data.items():
            game = yaml_games_by_name[name]
            if builtin_games and game not in builtin_games:
                norm_game   = _norm(game or "")
                has_apworld = any(
                    nk in norm_game or norm_game in nk
                    for nk in apworld_keys_norm
                )
                if not has_apworld:
                    uploader = result.yaml_uploaders.get(name)
                    mention  = uploader.mention if uploader else ""
                    result.issues.append((
                        f"missing_apworld:{name}",
                        f"{mention} ⚠️ **{name}**: game \"{game}\" is not a built-in world — please provide a `.apworld` file for it.",
                    ))
            else:
                yamls_to_validate[name] = data

        for yaml_name, yaml_bytes in result.yaml_data.items():
            _, game_reqs = get_yaml_requires(yaml_bytes)
            for req_game, req_ver in game_reqs.items():
                if builtin_games and req_game in builtin_games:
                    continue
                norm_req = _norm(req_game)
                matching_stem = next(
                    (s for s in apworld_keys_norm if s in norm_req or norm_req in s),
                    None,
                )
                if matching_stem is None:
                    continue
                apworld_name = apworld_keys_norm[matching_stem]
                info = apworld_infos[apworld_name]
                if info["world_version"] and req_ver:
                    if parse_version(info["world_version"]) < parse_version(req_ver):
                        uploader = result.yaml_uploaders.get(yaml_name)
                        mention  = uploader.mention if uploader else ""
                        result.issues.append((
                            f"apworld_version_too_old:{yaml_name}:{req_game}",
                            f"{mention} ⚠️ **{yaml_name}**: requires `{req_game}` version "
                            f"`{req_ver}` or newer, but the provided `.apworld` is version "
                            f"`{info['world_version']}`.",
                        ))

        if yamls_to_validate:
            loop = asyncio.get_running_loop()
            try:
                validation = await loop.run_in_executor(None, check_yamls_on_server, yamls_to_validate)
                for filename, verdict in validation.items():
                    if verdict != "Valid":
                        uploader = result.yaml_uploaders.get(filename)
                        mention  = uploader.mention if uploader else ""
                        result.issues.append((
                            f"yaml_invalid:{filename}",
                            f"{mention} ⚠️ **{filename}**: {html.unescape(verdict)}",
                        ))
            except Exception as e:
                result.issues.append((
                    "validation_unreachable",
                    f"⚠️ Could not reach archipelago.gg/check: `{e}`",
                ))

    return result
