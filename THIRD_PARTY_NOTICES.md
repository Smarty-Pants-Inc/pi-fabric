# Third-party notices

## Bend 2.0.26

The generated policy kernel includes Bend's emitted JavaScript trampoline and
compiled Base primitives from [Bend 2.0.26](https://github.com/bendlang/bend/tree/v2.0.26),
Copyright 2026 HigherOrderCO, licensed under Apache-2.0. Pi Fabric removes the
CLI invocation, adds library exports, and tree-shakes unused runtime code.
The complete license is distributed at [docs/licenses/bend-apache-2.0.txt](docs/licenses/bend-apache-2.0.txt).


Pi Fabric's QuickJS host-bridge design and TypeScript code-mode ergonomics were informed by the MIT-licensed [`boozedog/pi-codemode`](https://github.com/boozedog/pi-codemode) project.

Its subagent transport design was informed by the MIT-licensed [`sigilmakes/spindle`](https://github.com/sigilmakes/spindle), Pi's bundled subagent example, and LocalTerm's tmux-parity session API.

The workflow vocabulary was informed by Anthropic's dynamic workflows documentation and the MIT-licensed [`Michaelliv/pi-dynamic-workflows`](https://github.com/Michaelliv/pi-dynamic-workflows). The durable coordination model was informed by the MIT-licensed [`pi-messenger-swarm`](https://github.com/monotykamary/pi-messenger-swarm). Registered-tool interception follows the registry-chokepoint approach explored by the MIT-licensed [`pi-namespace`](https://github.com/monotykamary/pi-namespace) and [`pi-lazy-extensions`](https://github.com/monotykamary/pi-lazy-extensions) projects.

Pi Fabric adapts cooperative shell rendering, core-tool preview, diff, path-list, warning, and word-emphasis logic from the MIT-licensed [`pi-code-previews`](https://github.com/mattleong/pi-code-previews) package.

## pi-code-previews

MIT License

Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## TypeBox

`dist/worker.js` and `dist/memory/file-worker.js` bundle code from the MIT-licensed [`typebox`](https://github.com/sinclairzx81/typebox) package. Pi loads every other Fabric module with its own TypeBox.

MIT License

Copyright (c) 2017-2026 Haydn Paterson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
