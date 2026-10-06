export const EXCHANGE = 'new_message';

// Un evento por destino: cada consumer escucha solo el suyo
export const RK_GUARDAR_NAS = 'documento.guardar.nas';
export const RK_GUARDAR_BD = 'documento.guardar.bd';

// Los consumers responden aqui cuando terminan (exito o error)
export const RK_RESULTADO = 'documento.resultado';
export const RESULTADOS_QUEUE = 'publisher_resultados_queue';

export interface ResultadoMsg {
  eventId: string;
  documentoId: string;
  destino: 'NAS' | 'BD';
  ok: boolean;
  error?: string;
}

// La NAS recibe trozos: 1 mensaje con hasta CHUNK_NAS documentos del mismo lote
export const RK_GUARDAR_NAS_LOTE = 'documento.guardar.nas.lote';
export const CHUNK_NAS = 50;

export interface NasItemMsg {
  eventId: string;
  documentoId: string;
  title: string | null;
  author: string | null;
  createdAt: string;
  filename: string;
  mimeType: string;
  fileSize: number;
  reintento: number;
}

export interface NasLoteMsg {
  loteId: string;
  items: NasItemMsg[];
}
