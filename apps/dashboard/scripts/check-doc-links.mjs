// Check that every relative link and image in the project's Markdown resolves,
// including #anchors (GitHub heading slugs). Usage: node scripts/check-doc-links.mjs [repoRoot]
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'

const root = resolve(process.argv[2] ?? '../..')
const SKIP = new Set(['node_modules', '.git', '.venv', 'venv', 'legacy', '.claude', '.impeccable', 'dist', '.pytest_cache', '.ruff_cache'])

function walk(dir, out = []) {
  for (const name of readdirSync(dir)) {
    if (SKIP.has(name)) continue
    const p = join(dir, name)
    if (statSync(p).isDirectory()) walk(p, out)
    else if (name.endsWith('.md')) out.push(p)
  }
  return out
}

const slug = (h) =>
  h
    .trim()
    .toLowerCase()
    .replace(/<[^>]+>/g, '')
    .replace(/[^\p{L}\p{N}\s-]/gu, '')
    .replace(/\s/g, '-')

function anchorsOf(file) {
  const text = readFileSync(file, 'utf8').replace(/```[\s\S]*?```/g, '')
  const seen = new Map()
  const out = new Set()
  for (const m of text.matchAll(/^#{1,6}\s+(.+)$/gm)) {
    const base = slug(m[1])
    const n = seen.get(base) ?? 0
    seen.set(base, n + 1)
    out.add(n ? `${base}-${n}` : base)
  }
  return out
}

const problems = []
let checked = 0
for (const file of walk(root)) {
  const text = readFileSync(file, 'utf8').replace(/```[\s\S]*?```/g, '')
  const links = [...text.matchAll(/!?\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)/g), ...text.matchAll(/<img[^>]+src="([^"]+)"/g)].map((m) => m[1])
  for (const link of links) {
    if (/^(https?:|mailto:)/.test(link)) continue
    checked++
    const [pathPart, anchor] = link.split('#')
    const target = pathPart ? resolve(dirname(file), decodeURIComponent(pathPart)) : file
    const where = `${relative(root, file)} -> ${link}`
    if (!existsSync(target)) {
      problems.push(`missing: ${where}`)
      continue
    }
    if (anchor && target.endsWith('.md') && !anchorsOf(target).has(anchor)) problems.push(`no anchor: ${where}`)
  }
}
console.log(`checked ${checked} relative links in ${walk(root).length} Markdown files`)
if (problems.length) {
  console.error(problems.join('\n'))
  process.exitCode = 1
} else console.log('all links resolve')
