/*
 * Build the SQS Connector web admin plugin.
 *
 * The browser can't run JSX and the engine serves webadmin/ files raw (the
 * maven-resources-plugin copies them verbatim, filtering=false), so the JSX
 * source web/plugin.jsx must be compiled to web/plugin.js — the file
 * plugin.json's client.entry points at.
 *
 * NOTE: run `npm run build` (from the webadmin/ root) BEFORE `mvn package` so
 * the freshly-built web/plugin.js is the one packaged into the extension zip.
 * Committing the built web/plugin.js is sufficient for now; the Maven build is
 * intentionally not restructured (no frontend-maven-plugin wired in).
 *
 * @oie/* are left EXTERNAL — they are resolved by the host page's import map at
 * runtime (one shared framework instance), never bundled here.
 */
import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const dir = path.dirname(fileURLToPath(import.meta.url));

await build({
    entryPoints: [path.join(dir, 'plugin.jsx')],
    outfile: path.join(dir, 'plugin.js'),
    bundle: true,
    format: 'esm',
    target: 'es2022',
    jsx: 'transform',
    jsxFactory: 'React.createElement',
    jsxFragment: 'React.Fragment',
    external: ['@oie/web-api', '@oie/web-ui', '@oie/web-shell']
});

console.log('Built web/plugin.js');
