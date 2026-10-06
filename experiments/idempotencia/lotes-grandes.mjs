// Lotes grandes: node lotes-grandes.mjs [archivosPorLote=100] [lotes=2]
// Sube N archivos por lote, sigue el avance en vivo y luego reenvia el primer
// lote con la misma Idempotency-Key para comprobar que no se duplica nada.
import { execSync } from 'node:child_process';

const API = process.env.API ?? 'http://localhost:3000';
const N = Number(process.argv[2] ?? 100);
const LOTES = Number(process.argv[3] ?? 2);
const run = Date.now().toString(36);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const archivos = (lote) =>
  Array.from({ length: N }, (_, i) => [
    `lote${lote}-${String(i + 1).padStart(3, '0')}-${run}.txt`,
    `lote ${lote} archivo ${i + 1} run ${run}\n`.repeat(20),
  ]);

async function subir(files, key) {
  const fd = new FormData();
  for (const [name, content] of files) fd.append('files', new Blob([content]), name);
  const t0 = Date.now();
  const res = await fetch(`${API}/message/documentos`, {
    method: 'POST',
    body: fd,
    headers: { 'Idempotency-Key': key },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return { ...json.data, ms: Date.now() - t0 };
}

const estado = async (loteId) =>
  (await fetch(`${API}/outbox/lotes/${loteId}`)).json();

async function seguir(nombre, loteId, timeoutMs = 180_000) {
  const t0 = Date.now();
  let ultimo = '';
  while (Date.now() - t0 < timeoutMs) {
    const l = await estado(loteId);
    const linea =
      `[${nombre}] NAS ok ${l.resumen.NAS.CONFIRMED}/${N}  BD ok ${l.resumen.BD.CONFIRMED}/${N}` +
      `  enviados ${l.resumen.NAS.SENT + l.resumen.BD.SENT}  pendientes ${l.resumen.NAS.PENDING + l.resumen.BD.PENDING}` +
      `  FAILED ${l.resumen.NAS.FAILED + l.resumen.BD.FAILED}`;
    if (linea !== ultimo) console.log(`  ${((Date.now() - t0) / 1000).toFixed(0).padStart(3)}s ${linea}`);
    ultimo = linea;
    const terminado = l.resumen.NAS.CONFIRMED + l.resumen.NAS.FAILED === N &&
      l.resumen.BD.CONFIRMED + l.resumen.BD.FAILED === N;
    if (terminado) return { ...l, segundos: (Date.now() - t0) / 1000 };
    await sleep(1000);
  }
  return { ...(await estado(loteId)), segundos: timeoutMs / 1000, timeout: true };
}

const countBd = () =>
  Number(
    execSync('docker exec -i rabbit-test-producer-outbox-db-1 psql -U postgres -d postgres -At', {
      input: `select count(*) from "Documentos" where filename like '%-${run}.txt'`,
      encoding: 'utf8',
    }).trim(),
  );

console.log(`Run ${run}: ${LOTES} lotes x ${N} archivos`);
const lotes = [];
for (let l = 1; l <= LOTES; l++) {
  console.log(`\n== Lote ${l}: subiendo ${N} archivos`);
  const r = await subir(archivos(l), `lote${l}-${run}`);
  console.log(`  registrado en ${r.ms} ms, loteId=${r.loteId}, duplicados=${r.documentos.filter((d) => d.duplicado).length}`);
  lotes.push(r);
}

console.log('\n== Esperando que los consumers terminen (ver terminales de rabbit-test y rabbit-test-2)');
const finales = await Promise.all(lotes.map((r, i) => seguir(`lote ${i + 1}`, r.loteId)));
finales.forEach((f, i) =>
  console.log(`  lote ${i + 1}: completo=${f.completo} en ${f.segundos.toFixed(0)}s ${f.timeout ? '(TIMEOUT)' : ''}`),
);
console.log(`  filas en Documentos (BD): ${countBd()} / ${N * LOTES}`);

console.log('\n== Reenviando el lote 1 con la MISMA Idempotency-Key');
const eventosAntes = (await (await fetch(`${API}/outbox/stats`)).json()).documentos;
const rep = await subir(archivos(1), `lote1-${run}`);
const eventosDespues = (await (await fetch(`${API}/outbox/stats`)).json()).documentos;
const dup = rep.documentos.filter((d) => d.duplicado).length;
console.log(`  duplicados: ${dup}/${N}  (${dup === N ? 'OK' : 'FALLO'})`);
console.log(`  documentos en outbox: ${eventosAntes} -> ${eventosDespues}  (${eventosAntes === eventosDespues ? 'OK' : 'FALLO'})`);
await sleep(3000);
console.log(`  filas en Documentos (BD): ${countBd()} / ${N * LOTES}`);
console.log('\nStats finales:', JSON.stringify((await (await fetch(`${API}/outbox/stats`)).json()).porDestino));
