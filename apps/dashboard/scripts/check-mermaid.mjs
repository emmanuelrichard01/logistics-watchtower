// Parse every ```mermaid block with Mermaid's own parser (what GitHub renders with).
// Usage: npm install --no-save mermaid@11 && node scripts/check-mermaid.mjs <file.md>...
import { chromium } from '@playwright/test'
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'

const mermaidPath = createRequire(import.meta.url).resolve('mermaid/dist/mermaid.min.js')
const browser = await chromium.launch()
const page = await browser.newPage()
await page.addScriptTag({ path: mermaidPath })
let n = 0
let bad = 0
for (const f of process.argv.slice(2)) {
  const blocks = [...readFileSync(f, 'utf8').matchAll(/```mermaid\r?\n([\s\S]*?)```/g)].map((m) => m[1])
  for (const [i, src] of blocks.entries()) {
    n++
    const err = await page.evaluate(async (s) => {
      try {
        await window.mermaid.parse(s)
        return null
      } catch (e) {
        return String(e?.message ?? e).slice(0, 300)
      }
    }, src)
    if (err) {
      bad++
      console.log(`FAIL ${f} block ${i + 1}: ${err}`)
    }
  }
}
console.log(`${n} mermaid blocks, ${bad} failing`)
await browser.close()
process.exitCode = bad ? 1 : 0
