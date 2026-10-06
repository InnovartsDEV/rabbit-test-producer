import type { OutboxStatus } from '@prisma/client';

// Reintentos automaticos cuando NAS y BD fallan a la vez (espera 5s, 10s, 20s)
export const MAX_REINTENTOS = 3;
export const REINTENTO_BASE_MS = 5_000;

/**
 * Estado del documento de cara al usuario:
 *  - COMPLETO      NAS y BD guardaron
 *  - PARCIAL       uno guardo y el otro fallo -> se considera exito (200)
 *  - EN_PROCESO    hay eventos pendientes de publicar o de confirmar
 *  - REINTENTANDO  fallaron los dos, esperando el siguiente reintento
 *  - ERROR         fallaron los dos y se agotaron los reintentos
 */
export type EstadoDocumento =
  | 'COMPLETO'
  | 'PARCIAL'
  | 'EN_PROCESO'
  | 'REINTENTANDO'
  | 'ERROR';

export function estadoDocumento(
  eventos: { status: OutboxStatus }[],
  reintentos: number,
): EstadoDocumento {
  const confirmados = eventos.filter((e) => e.status === 'CONFIRMED').length;
  const fallidos = eventos.filter((e) => e.status === 'FAILED').length;

  if (confirmados === eventos.length) return 'COMPLETO';
  if (confirmados + fallidos < eventos.length) return 'EN_PROCESO';
  if (confirmados > 0) return 'PARCIAL';
  return reintentos < MAX_REINTENTOS ? 'REINTENTANDO' : 'ERROR';
}

/** Codigo HTTP que corresponde a cada estado al consultar un documento. */
export function httpStatusDe(estado: EstadoDocumento): number {
  switch (estado) {
    case 'COMPLETO':
    case 'PARCIAL':
      return 200;
    case 'EN_PROCESO':
    case 'REINTENTANDO':
      return 202;
    case 'ERROR':
      return 502;
  }
}
