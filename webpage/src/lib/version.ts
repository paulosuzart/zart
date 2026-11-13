import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

/**
 * Locate and parse the root workspace Cargo.toml so the website and docs
 * stay synchronized with [workspace.package].version as a single source of truth.
 */
function loadWorkspaceMetadata(): { zartVersion: string; sqlxVersion: string } {
  const candidates = [
    resolve(process.cwd(), "Cargo.toml"),
    resolve(process.cwd(), "../Cargo.toml"),
    resolve(process.cwd(), "../../Cargo.toml"),
  ];

  for (const candidate of candidates) {
    if (!existsSync(candidate)) continue;
    const raw = readFileSync(candidate, "utf8");

    const pkgSection = raw.match(/\[workspace\.package\][\s\S]*?(?=\n\[|$)/)?.[0] ?? "";
    const zartVersion =
      pkgSection.match(/^\s*version\s*=\s*"([^"]+)"/m)?.[1] ?? "0.2.1";

    const sqlxVersion =
      raw.match(/^\s*sqlx\s*=\s*\{[^}]*version\s*=\s*"([^"]+)"/m)?.[1] ?? "0.9";

    return { zartVersion, sqlxVersion };
  }

  return { zartVersion: "0.2.1", sqlxVersion: "0.9" };
}

const meta = loadWorkspaceMetadata();

export const ZART_VERSION = meta.zartVersion;
export const ZART_MINOR = meta.zartVersion.split(".").slice(0, 2).join(".");
export const SQLX_VERSION = meta.sqlxVersion;
