import { readFileSync } from "fs";
import { equal } from "assert";

async function main() {
  const wasm = readFileSync("./crdt.wasm");

  const module = await WebAssembly.instantiate(wasm);
  const {
    doc_new,
    doc_counter,
    counter_inc,
    counter_dec,
    counter_read,
  } = module.instance.exports;


  const doc1 = doc_new();
  console.log(doc1)
  const counter1 = doc_counter(doc1, 'counter', 42);
  counter_inc(counter1);
  counter_inc(counter1);
  counter_dec(counter1);
  const value1 = counter_read(counter1);
  equal(value1, 43);
  console.log(value1);

  const doc2 = doc_new();
  console.log(doc2)
  const counter2 = doc_counter(doc2, 'counter', 42);
  counter_inc(counter2);
  counter_dec(counter2);
  counter_dec(counter2);
  const value2 = counter_read(counter2);
  equal(value2, 41);
  console.log(value2);
}

main();
