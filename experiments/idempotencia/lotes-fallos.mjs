// Simulacion de fallos: node lotes-fallos.mjs [archivos=100] [temporal]
// Reparte los archivos en 4 grupos iguales (25% c/u) segun la etiqueta del nombre:
//   ok          NAS ok     BD ok      -> COMPLETO              HTTP 200
//   falla-bd    NAS ok     BD falla   -> PARCIAL (exito)       HTTP 200, sin reintento
//   falla-nas   NAS falla  BD ok      -> PARCIAL (exito)       HTTP 200, sin reintento
//   falla-ambos NAS falla  BD falla   -> reintenta 3 veces     HTTP 502 al agotarse
// Con el argumento `temporal`, el grupo "ambos" falla solo la 1a vez y el
// reintento lo recupera -> COMPLETO, reintentos=1, HTTP 200.
// Requiere SIMULAR_FALLOS=true en rabbit-test y rabbit-test-2.
import { execSync } from 'node:child_process';

const API = process.env.API ?? 'http://localhost:3000';
const N = Number(process.argv[2] ?? 100);
const TEMPORAL = process.argv.includes('temporal');
const run = Date.now().toString(36);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const AMBOS = TEMPORAL ? 'falla-ambos-temporal' : 'falla-ambos';
const TAGS = ['ok', 'falla-bd', 'falla-nas', AMBOS];
const ESPERADO = {
  ok: { estado: 'COMPLETO', http: 200, reintentos: 0 },
  'falla-bd': { estado: 'PARCIAL', http: 200, reintentos: 0 },
  'falla-nas': { estado: 'PARCIAL', http: 200, reintentos: 0 },
  [AMBOS]: TEMPORAL
    ? { estado: 'COMPLETO', http: 200, reintentos: 1 }
    : { estado: 'ERROR', http: 502, reintentos: 3 },
};

const psql = (db, sql) =>
  execSync(`docker exec -i rabbit-test-producer-outbox-db-1 psql -U postgres -d ${db} -At`, {
    input: sql,
    encoding: 'utf8',
  }).trim();

const files = Array.from({ length: N }, (_, i) => {
  const tag = TAGS[i % 4];
  const name = `doc-${String(i + 1).padStart(3, '0')}__${tag}__${run}.txt`;
  return [name, `documento ${i + 1} ${tag} run ${run}\n`.repeat(20)];
});

console.log(`Run ${run}: 1 lote de ${N} archivos (${N / 4} por escenario)${TEMPORAL ? ' [fallo ambos TEMPORAL]' : ''}`);
const fd = new FormData();
for (const [name, content] of files) fd.append('files', new Blob([content]), name);
const res = await fetch(`${API}/message/documentos`, {
  method: 'POST',
  body: fd,
  headers: { 'Idempotency-Key': `fallos-${run}` },
});
const { data } = await res.json();
console.log(`registrado: loteId=${data.loteId}\n`);

const t0 = Date.now();
let l;
while (Date.now() - t0 < 600_000) {
  l = await (await fetch(`${API}/outbox/lotes/${data.loteId}`)).json();
  const e = l.porEstado;
  console.log(
    `  ${((Date.now() - t0) / 1000).toFixed(0).padStart(3)}s  completos ${e.COMPLETO}  parciales ${e.PARCIAL}  en proceso ${e.EN_PROCESO}  reintentando ${e.REINTENTANDO}  error ${e.ERROR}`,
  );
  if (l.terminado) break;
  await sleep(2000);
}
console.log(`\nTiempo total hasta estado final: ${((Date.now() - t0) / 1000).toFixed(0)}s`);

// Estado final de cada documento tal como lo veria el usuario (GET por id)
const obtenido = Object.fromEntries(TAGS.map((t) => [t, {}]));
for (let i = 0; i < data.documentos.length; i++) {
  const r = await fetch(`${API}/outbox/documentos/${data.documentos[i].id}`);
  const doc = await r.json();
  const clave = `${doc.estado} http=${r.status} reintentos=${doc.reintentos}`;
  const tag = TAGS[i % 4];
  obtenido[tag][clave] = (obtenido[tag][clave] ?? 0) + 1;
}

console.log('\nEscenario              Resultado observado (documentos)                         Esperado');
let todoOk = true;
for (const tag of TAGS) {
  const e = ESPERADO[tag];
  const esperadoClave = `${e.estado} http=${e.http} reintentos=${e.reintentos}`;
  const ok = Object.keys(obtenido[tag]).length === 1 && obtenido[tag][esperadoClave] === N / 4;
  todoOk &&= ok;
  console.log(
    `${tag.padEnd(22)} ${Object.entries(obtenido[tag]).map(([k, v]) => `${v} x ${k}`).join(', ').padEnd(56)} ${esperadoClave}  ${ok ? 'OK' : 'FALLO'}`,
  );
}

// BD guarda: ok + falla-nas (+ ambos si se recupera en el reintento). falla-bd nunca.
const bdEsperado = TEMPORAL ? (N / 4) * 3 : N / 2;
const enBd = Number(
  psql('postgres', `select count(*) from "Documentos" where filename like '%__${run}.txt'`),
);
const bdOk = enBd === bdEsperado;
todoOk &&= bdOk;
console.log(`\nFilas en Documentos (BD): ${enBd} (esperado ${bdEsperado})  ${bdOk ? 'OK' : 'FALLO'}`);

const intentos = psql(
  'outbox',
  `select max(d.reintentos) from "Documento" d where d."loteId"='${data.loteId}'`,
);
console.log(`Maximo de reintentos usados: ${intentos}`);
console.log(`\n${todoOk ? 'RESULTADO: OK' : 'RESULTADO: HAY DIFERENCIAS'}`);
