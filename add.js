const fs = require("fs");

async function main() {
  const wasm = fs.readFileSync("./add.wasm");

  const module = await WebAssembly.instantiate(wasm);

  const { add } = module.instance.exports;

  console.log(add(2, 3));
}

main();
