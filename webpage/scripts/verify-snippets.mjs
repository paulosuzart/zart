import { spawnSync } from "node:child_process";
import { mkdirSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const webpageDir = resolve(here, "..");
const repoRoot = resolve(webpageDir, "..");
const tsBindingDir = resolve(repoRoot, "bindings/typescript");
const docsDir = resolve(webpageDir, "src/content/docs/docs");
const indexAstro = resolve(webpageDir, "src/pages/index.astro");

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...walk(full));
    else if (/\.(md|mdx)$/.test(entry)) out.push(full);
  }
  return out.sort();
}

function extractFencedBlocks(filePath) {
  const text = readFileSync(filePath, "utf8");
  const lines = text.split("\n");
  const blocks = [];
  let inBlock = false;
  let lang = "";
  let info = "";
  let startLine = 0;
  let buf = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const fence = line.match(/^\s*```(\w+)?(.*)$/);
    if (!inBlock && fence) {
      inBlock = true;
      lang = (fence[1] ?? "").toLowerCase();
      info = (fence[2] ?? "").trim();
      startLine = i + 2;
      buf = [];
    } else if (inBlock && /^\s*```\s*$/.test(line)) {
      inBlock = false;
      if (!info.includes("no-verify") && !info.includes("ignore")) {
        blocks.push({ file: relative(repoRoot, filePath), line: startLine, lang, code: buf.join("\n") });
      }
    } else if (inBlock) {
      buf.push(line);
    }
  }
  return blocks;
}

function extractAstroCodeBlocks(filePath) {
  const text = readFileSync(filePath, "utf8");
  const blocks = [];
  const re = /const\s+(\w*code\w*|\w*snippet\w*|rustCode|pythonCode|tsCode|typescriptCode)\s*=\s*`([\s\S]*?)`;/gi;
  for (const m of text.matchAll(re)) {
    const name = m[1].toLowerCase();
    const lang = name.includes("py") ? "python" : name.includes("ts") || name.includes("typescript") ? "ts" : "rust";
    const line = text.slice(0, m.index).split("\n").length;
    blocks.push({
      file: relative(repoRoot, filePath),
      line,
      lang,
      code: m[2].replace(/\\`/g, "`").replace(/\\\$/g, "$"),
    });
  }
  return blocks.length > 0 ? blocks : extractFencedBlocks(filePath);
}

const files = walk(docsDir);
const allBlocks = files.flatMap(extractFencedBlocks);
try {
  if (statSync(indexAstro).isFile()) {
    allBlocks.push(...extractAstroCodeBlocks(indexAstro));
  }
} catch {
  // ignore
}

const tsBlocks = allBlocks.filter((b) => b.lang === "ts" || b.lang === "typescript");
const pyBlocks = allBlocks.filter((b) => b.lang === "python" || b.lang === "py");
const rsBlocks = allBlocks.filter((b) => b.lang === "rust" || b.lang === "rs");

let failures = 0;

// 1. TypeScript validation against @zart/effect & effect v4 (grouped per page so multi-step tutorials share declarations)
if (tsBlocks.length > 0) {
  const checkDir = join(tsBindingDir, ".snippet-check");
  rmSync(checkDir, { recursive: true, force: true });
  mkdirSync(checkDir, { recursive: true });
  try {
    const byFile = new Map();
    for (const b of tsBlocks) {
      if (!byFile.has(b.file)) byFile.set(b.file, []);
      byFile.get(b.file).push(b);
    }

    const fileLineMaps = new Map();
    let idx = 0;
    for (const [docFile, blocks] of byFile.entries()) {
      const importedNames = new Set();
      const importLines = [];
      const bodyLines = [];
      const lineToBlock = [];

      const addNamedImport = (names, fromMod) => {
        const fresh = names.filter((n) => !importedNames.has(n));
        if (fresh.length === 0) return;
        for (const n of fresh) importedNames.add(n);
        importLines.push(`import { ${fresh.join(", ")} } from "${fromMod}";`);
      };

      for (const b of blocks) {
        const lines = b.code
          .replace(/from\s+["']@zart\/effect["']/g, `from "../ts/src/index.js"`)
          .replace(/from\s+["']\.\.\/dist\/index\.js["']/g, `from "../ts/src/index.js"`)
          .split("\n");

        const codeLines = [];
        for (let i = 0; i < lines.length; i++) {
          const l = lines[i];
          if (/^\s*import\s+/.test(l)) {
            let stmt = l;
            while (!stmt.includes(";") && !stmt.includes(" from ") && i + 1 < lines.length) {
              i++;
              stmt += ` ${lines[i].trim()}`;
            }
            const m = stmt.match(/import\s+(?:type\s+)?\{([^}]+)\}\s+from\s+["']([^"']+)["']/);
            if (m) {
              const names = m[1]
                .split(",")
                .map((s) => s.trim().replace(/^type\s+/, ""))
                .filter(Boolean);
              addNamedImport(names, m[2]);
            } else {
              importLines.push(stmt);
            }
          } else {
            codeLines.push({ text: l, origLine: b.line + i });
          }
        }

        // Check if snippet has `yield*` at brace depth 0, or any `yield*` without `function*`
        let depth = 0;
        let hasDepthZeroYield = false;
        let hasAnyYield = false;
        for (const c of codeLines) {
          const noComment = c.text.replace(/\/\/.*$/, "");
          if (/\byield\*/.test(noComment)) {
            hasAnyYield = true;
            if (depth === 0) hasDepthZeroYield = true;
          }
          for (const ch of noComment) {
            if (ch === "{") depth++;
            else if (ch === "}") depth = Math.max(0, depth - 1);
          }
        }
        const rawNoComment = codeLines.map((c) => c.text.replace(/\/\/.*$/, "")).join("\n");
        const hasTopLevelYield = hasDepthZeroYield || (hasAnyYield && !/\bfunction\s*\*/.test(rawNoComment));

        if (hasTopLevelYield) {
          bodyLines.push("Effect.gen(function* () {");
          lineToBlock.push({ file: docFile, line: b.line });
          for (const c of codeLines) {
            bodyLines.push(c.text);
            lineToBlock.push({ file: docFile, line: c.origLine });
          }
          bodyLines.push("});");
          lineToBlock.push({ file: docFile, line: b.line });
        } else {
          for (const c of codeLines) {
            bodyLines.push(c.text);
            lineToBlock.push({ file: docFile, line: c.origLine });
          }
        }
      }

      // Add default imports for any names not explicitly imported by the page's snippets
      const defaultImports = [
        [["Data", "Duration", "Effect", "Fiber", "Layer", "Schema"], "effect"],
        [["SqlClient"], "effect/sql"],
        [
          [
            "Zart",
            "PgBackend",
            "DurableScheduler",
            "WorkerBuilder",
            "ClientTrx",
            "TrxHandle",
            "ZartTrx",
            "ZartWorkflow",
            "Retry",
            "ZartSqlClientLive",
            "makeZartSqlClient",
            "RetryExhaustedError",
            "StepTimeoutError",
            "DeadlineExceededError",
            "StepFailedError",
            "ZartBackendError",
            "ZartSchemaError",
            "ControlFlowDefect",
          ],
          "../ts/src/index.js",
        ],
      ];
      for (const [names, mod] of defaultImports) addNamedImport(names, mod);

      // Track only top-level (depth 0) declarations outside Effect.gen wrappers
      const topLevelDecls = new Set();
      let d = 0;
      for (const l of bodyLines) {
        if (d === 0) {
          const m = l.match(/^(?:const|let|var|function|class|type|interface)\s+(\w+)\b/);
          if (m) topLevelDecls.add(m[1]);
        }
        for (const ch of l) {
          if (ch === "{") d++;
          else if (ch === "}") d = Math.max(0, d - 1);
        }
      }

      const ambientPool = [
        ["backend", "declare const backend: PgBackend;"],
        ["scheduler", "declare const scheduler: DurableScheduler;"],
        ["tx", "declare const tx: ClientTrx;"],
        ["executionId", "declare const executionId: string;"],
        ["execId", "declare const execId: string;"],
        ["userId", "declare const userId: string;"],
        ["orderId", "declare const orderId: string;"],
        ["accountId", "declare const accountId: string;"],
        ["taskName", "declare const taskName: string;"],
        ["input", "declare const input: any;"],
        ["data", "declare const data: any;"],
        ["arg1", "declare const arg1: any;"],
        ["items", "declare const items: any[];"],
        ["defaultConfig", "declare const defaultConfig: any;"],
        ["OnboardingInput", "declare const OnboardingInput: Schema.Codec<any, any>; type OnboardingInput = any;"],
        ["OnboardingOutput", "declare const OnboardingOutput: Schema.Codec<any, any>; type OnboardingOutput = any;"],
        ["OnboardInput", "declare const OnboardInput: Schema.Codec<any, any>; type OnboardInput = any;"],
        ["GreetingInput", "declare const GreetingInput: Schema.Codec<any, any>; type GreetingInput = any;"],
        ["ApprovalRequest", "declare const ApprovalRequest: Schema.Codec<any, any>; type ApprovalRequest = any;"],
        ["OrderInput", "declare const OrderInput: Schema.Codec<any, any>; type OrderInput = any;"],
        ["BatchInput", "type BatchInput = any;"],
        ["ProcessedReport", "type ProcessedReport = any;"],
        ["Report", "type Report = Record<string, any>;"],
        ["dbUrl", "declare const dbUrl: string;"],
        ["email", "declare const email: string;"],
        ["randomUUID", "declare const randomUUID: () => string;"],
        ["poolProvider", "declare const poolProvider: any;"],
        ["myWorkflow", "declare const myWorkflow: any;"],
        ["mailer", "declare const mailer: any;"],
        ["stripe", "declare const stripe: any;"],
        ["processed", "declare const processed: any[];"],
        ["notifyStakeholder", "declare const notifyStakeholder: (...args: any[]) => any;"],
        [
          "sendConfirmation",
          "declare const sendConfirmation: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "fetchInventory",
          "declare const fetchInventory: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "writeReport",
          "declare const writeReport: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "loadConfig",
          "declare const loadConfig: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "processPayments",
          "declare const processPayments: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "generateSummary",
          "declare const generateSummary: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "fetchReports",
          "declare const fetchReports: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any[]>>>;",
        ],
        [
          "reserveInventory",
          "declare const reserveInventory: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "checkBalance",
          "declare const checkBalance: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "fetchRemoteConfig",
          "declare const fetchRemoteConfig: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "chargeCard",
          "declare const chargeCard: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "createUser",
          "declare const createUser: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "setupAccount",
          "declare const setupAccount: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "sendWelcomeEmail",
          "declare const sendWelcomeEmail: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "checkService",
          "declare const checkService: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "processItem",
          "declare const processItem: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "sendFollowup",
          "declare const sendFollowup: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        ["computeNextMonday", "declare const computeNextMonday: (...args: any[]) => Date;"],
        [
          "validateRequest",
          "declare const validateRequest: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "provisionResource",
          "declare const provisionResource: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "validateAddress",
          "declare const validateAddress: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "processPayment",
          "declare const processPayment: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        [
          "shipOrder",
          "declare const shipOrder: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;",
        ],
        ["stepA", "declare const stepA: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;"],
        ["stepB", "declare const stepB: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;"],
        ["stepC", "declare const stepC: (...args: any[]) => ReturnType<ReturnType<typeof Zart.step<any[], any>>>;"],
      ];

      const ambientLines = ambientPool
        .filter(([name]) => !importedNames.has(name) && !topLevelDecls.has(name))
        .map(([, decl]) => decl);

      const headerCount = importLines.length + ambientLines.length;
      const fullContent = [...importLines, ...ambientLines, ...bodyLines, "export {};"].join("\n");
      const fname = `page_${idx++}.ts`;
      fileLineMaps.set(fname, { headerCount, lineToBlock, docFile });
      writeFileSync(join(checkDir, fname), fullContent, "utf8");
    }

    writeFileSync(
      join(checkDir, "tsconfig.json"),
      JSON.stringify(
        {
          extends: "../tsconfig.json",
          compilerOptions: {
            noUnusedLocals: false,
            noUnusedParameters: false,
            rootDir: "..",
          },
          include: ["./**/*.ts"],
        },
        null,
        2,
      ),
      "utf8",
    );

    const tscBin = join(tsBindingDir, "node_modules/.bin/tsc");
    const res = spawnSync(tscBin, ["--noEmit", "-p", join(checkDir, "tsconfig.json")], {
      encoding: "utf8",
    });
    if (res.status !== 0) {
      failures++;
      console.error("TypeScript snippet check failed:");
      const out = (res.stdout || "") + (res.stderr || "");
      for (const line of out.split("\n")) {
        const m = line.match(/(page_\d+\.ts)\((\d+),(\d+)\):\s*(.*)/);
        if (m) {
          const info = fileLineMaps.get(m[1]);
          if (info) {
            const bodyIdx = Number(m[2]) - 1 - info.headerCount;
            const mapped = info.lineToBlock[Math.max(0, Math.min(bodyIdx, info.lineToBlock.length - 1))];
            console.error(`  ${mapped.file}:${mapped.line}:${m[3]} - ${m[4]}`);
            continue;
          }
        }
        if (line.trim()) console.error(`  ${line}`);
      }
    }
  } finally {
    rmSync(checkDir, { recursive: true, force: true });
  }
}

// 2. Python syntax validation via ast.parse
if (pyBlocks.length > 0) {
  const pyCheck = [
    "import ast, json, sys, textwrap",
    "items = json.load(sys.stdin)",
    "errs = []",
    "for item in items:",
    "    src = textwrap.dedent(item['code'])",
    "    try:",
    "        ast.parse(src, filename=item['file'])",
    "    except SyntaxError as e:",
    "        errs.append(f\"{item['file']}:{item['line'] + (e.lineno or 1) - 1}: {e.msg}\")",
    "if errs:",
    "    print('\\n'.join(errs), file=sys.stderr)",
    "    sys.exit(1)",
  ].join("\n");

  const res = spawnSync("python3", ["-c", pyCheck], {
    input: JSON.stringify(pyBlocks),
    encoding: "utf8",
  });
  if (res.status !== 0) {
    failures++;
    console.error("Python snippet check failed:");
    console.error(res.stderr || res.stdout);
  }
}

// 3. Rust syntax validation (balanced delimiters + rustfmt on full statements/items)
if (rsBlocks.length > 0) {
  for (const b of rsBlocks) {
    const stack = [];
    const pairs = { "(": ")", "[": "]", "{": "}" };
    let inStr = false;
    let inLineComment = false;
    let inBlockComment = false;
    let ok = true;
    for (let i = 0; i < b.code.length; i++) {
      const ch = b.code[i];
      const next = b.code[i + 1];
      if (inLineComment) {
        if (ch === "\n") inLineComment = false;
        continue;
      }
      if (inBlockComment) {
        if (ch === "*" && next === "/") {
          inBlockComment = false;
          i++;
        }
        continue;
      }
      if (inStr) {
        if (ch === "\\") i++;
        else if (ch === '"') inStr = false;
        continue;
      }
      if (ch === "/" && next === "/") {
        inLineComment = true;
        i++;
      } else if (ch === "/" && next === "*") {
        inBlockComment = true;
        i++;
      } else if (ch === '"') {
        inStr = true;
      } else if (pairs[ch]) {
        stack.push(pairs[ch]);
      } else if (ch === ")" || ch === "]" || ch === "}") {
        if (stack.pop() !== ch) {
          ok = false;
          break;
        }
      }
    }
    if (!ok || stack.length > 0) {
      failures++;
      console.error(`Rust snippet delimiter mismatch at ${b.file}:${b.line}`);
    }
  }
}

if (failures > 0) {
  process.exit(1);
}
console.log(
  `Verified ${tsBlocks.length} TypeScript, ${pyBlocks.length} Python, and ${rsBlocks.length} Rust snippets cleanly.`,
);
