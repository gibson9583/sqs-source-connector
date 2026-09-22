/* SPDX-License-Identifier: MPL-2.0 */
/*
 * Build the SQS Connector web admin plugin.
 *
 * The browser can't run JSX and the engine serves webadmin/ files raw (the
 * maven-resources-plugin copies them verbatim, filtering=false), so the JSX
 * source web/plugin.jsx must be compiled to web/plugin.js — the file
 * plugin.json's client.entry points at.
 *
 * Maven runs this build during generate-resources before packaging the bundle.
 *
 * @oie/* are left EXTERNAL — they are resolved by the host page's import map at
 * runtime (one shared framework instance), never bundled here.
 */
import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

const dir = path.dirname(fileURLToPath(import.meta.url));
// One release version across engine and web manifests, including versions:set.
const pom = readFileSync(path.resolve(dir, '../../../pom.xml'), 'utf8');
const version = pom.match(/<artifactId>sqs-connector<\/artifactId>\s*<version>([^<]+)<\/version>/)?.[1];
if (!version) throw new Error('Cannot read the SQS connector release version');
const manifestPath = path.resolve(dir, '../plugin.json');
const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
manifest.version = version;
writeFileSync(manifestPath, JSON.stringify(manifest, null, 4) + '\n');

await build({
    entryPoints: [path.join(dir, 'plugin.jsx')],
    outfile: path.join(dir, 'plugin.js'),
    bundle: true,
    format: 'esm',
    banner: { js: '/* SPDX-License-Identifier: MPL-2.0 */' },
    target: 'es2022',
    jsx: 'transform',
    jsxFactory: 'React.createElement',
    jsxFragment: 'React.Fragment',
    external: ['@oie/web-api', '@oie/web-ui', '@oie/web-shell']
});

console.log('Built web/plugin.js');
