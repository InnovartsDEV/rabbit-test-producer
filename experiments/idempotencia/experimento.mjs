// Experimento de idempotencia: lotes, reenvios y duplicados en el broker.
import { execSync } from 'node:child_process';

const API = 'http://localhost:3000';
const OUTBOX_DB = 'rabbit-test-producer-outbox-db-1';
const DOCS_DB = 'rabbitexp-documentos-db-1';
const run = Date.now().toString(36);

const psql = (container, db, sql) =>
  execSync(`docker exec -i ${container} psql -U postgres -d ${db} -At`, {
    input: sql,
    encoding: 'utf8',
  }).trim();
const nasFiles = () =>
  execSync('docker exec rabbitexp-sftp-1 find /home/nas/upload -type f', {
    encoding: 'utf8',
  })
    .split('\n')
    .filter((f) => f.includes(run));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const results = [];
const check = (name, ok, detail = '') => {
  results.push(ok);
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  -> ' + detail : ''}`);
};

async function subirLote(files, key) {
  const fd = new FormData();
  for (const [name, content] of files) fd.append('files', new Blob([content]), name);
  const res = await fetch(`${API}/message/documentos`, {
    method: 'POST',
    body: fd,
    headers: key ? { 'Idempotency-Key': key } : {},
  });
  return (await res.json()).data;
}

async function esperarLote(loteId, ms = 30000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    const l = await (await fetch(`${API}/outbox/lotes/${loteId}`)).json();
    if (l.completo) return l;
    await sleep(1000);
  }
  return await (await fetch(`${API}/outbox/lotes/${loteId}`)).json();
}

const countBd = (like) =>
  Number(psql(DOCS_DB, 'postgres', `select count(*) from "Documentos" where filename like '${like}'`));
const countOutbox = (loteId) =>
  Number(psql(OUTBOX_DB, 'outbox', `select count(*) from "OutboxEvent" e join "Documento" d on d.id=e."documentoId" where d."loteId"='${loteId}'`));
const tmpFiles = () =>
  execSync('ls "C:/Users/Eduardo Solano/Desktop/RabbitMQ/rabbit-test-producer/tmp-uploads" 2>/dev/null | wc -l', { encoding: 'utf8', shell: 'bash' }).trim();

// ---------------------------------------------------------------- A
console.log(`\n=== A. Lote de 3 archivos con Idempotency-Key (run ${run})`);
const key = `lote-${run}`;
const archivos = [1, 2, 3].map((i) => [`a${i}-${run}.txt`, `contenido ${i} ${run}`]);
const a = await subirLote(archivos, key);
const loteA = a.loteId;
check('3 documentos registrados, ninguno duplicado', a.documentos.length === 3 && a.documentos.every((d) => !d.duplicado));
const la = await esperarLote(loteA);
check('lote completo (6 eventos CONFIRMED)', la.completo, JSON.stringify(la.resumen));
check('BD: 3 filas en Documentos', countBd(`%-${run}.txt`) === 3, `${countBd(`%-${run}.txt`)} filas`);
check('NAS: 3 archivos', nasFiles().length === 3, `${nasFiles().length} archivos`);

// ---------------------------------------------------------------- B
console.log('\n=== B. Reenviar el MISMO lote con la misma Idempotency-Key');
const eventosAntes = Number(psql(OUTBOX_DB, 'outbox', 'select count(*) from "OutboxEvent"'));
const b = await subirLote(archivos, key);
const eventosDespues = Number(psql(OUTBOX_DB, 'outbox', 'select count(*) from "OutboxEvent"'));
check('los 3 vuelven como duplicado=true con los mismos ids',
  b.documentos.every((d, i) => d.duplicado && d.id === a.documentos[i].id));
check('no se crean eventos nuevos en el outbox', eventosAntes === eventosDespues, `${eventosAntes} -> ${eventosDespues}`);
await sleep(3000);
check('BD sigue con 3 filas', countBd(`%-${run}.txt`) === 3);
check('NAS sigue con 3 archivos', nasFiles().length === 3);

// ---------------------------------------------------------------- C
console.log('\n=== C. Duplicados en el broker: cada evento del lote publicado 2 veces mas');
const filas = psql(
  OUTBOX_DB, 'outbox',
  `select e.id||'|'||e."routingKey"||'|'||e.payload::text from "OutboxEvent" e join "Documento" d on d.id=e."documentoId" where d."loteId"='${loteA}'`,
).split('\n');
const publicar = (routingKey, obj) =>
  fetch('http://localhost:15672/api/exchanges/%2F/new_message/publish', {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: 'Basic ' + Buffer.from('admin:admin').toString('base64') },
    body: JSON.stringify({ properties: { content_type: 'application/json' }, routing_key: routingKey, payload: JSON.stringify(obj), payload_encoding: 'string' }),
  });
for (const fila of filas) {
  const [id, rk, ...resto] = fila.split('|');
  const payload = resto.join('|');
  for (let i = 0; i < 2; i++) await publicar(rk, { ...JSON.parse(payload), eventId: id });
}
await sleep(6000);
const lc = await (await fetch(`${API}/outbox/lotes/${loteA}`)).json();
check('BD: sigue en 3 filas (upsert por documentoId)', countBd(`%-${run}.txt`) === 3, `${countBd(`%-${run}.txt`)} filas`);
check('NAS: sigue en 3 archivos', nasFiles().length === 3);
check('outbox: los 6 eventos siguen CONFIRMED', lc.completo, JSON.stringify(lc.resumen));

// ---------------------------------------------------------------- D
console.log('\n=== D. Resultado FAILED tardio sobre un evento ya CONFIRMED');
const [evId, , evPayload] = [filas[0].split('|')[0], 0, JSON.parse(filas[0].split('|').slice(2).join('|'))];
await publicar('documento.resultado', { eventId: evId, documentoId: evPayload.documentoId, destino: 'NAS', ok: false, error: 'tardio' });
await sleep(3000);
const st = psql(OUTBOX_DB, 'outbox', `select status from "OutboxEvent" where id='${evId}'`);
check('el evento sigue CONFIRMED (no retrocede)', st === 'CONFIRMED', st);

// ---------------------------------------------------------------- E
console.log('\n=== E. Mismo nombre de archivo dos veces en un lote (contenidos distintos)');
const e = await subirLote([[`mismo-${run}.txt`, 'contenido UNO'], [`mismo-${run}.txt`, 'contenido DOS']], null);
await esperarLote(e.loteId);
const enBd = countBd(`mismo-${run}.txt`);
const enNas = nasFiles().filter((f) => f.includes(`mismo-${run}`)).length;
check('BD: 2 filas', enBd === 2, `${enBd}`);
check('NAS: 2 archivos (cada documento conserva el suyo)', enNas === 2, `${enNas} archivo(s) en NAS`);

console.log(`\nTemporales pendientes en tmp-uploads: ${tmpFiles()}`);
console.log(`\n${results.filter(Boolean).length}/${results.length} checks OK`);
