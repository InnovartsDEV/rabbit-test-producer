# Experimento de idempotencia

Infra extra (BD `Documentos` en :5432 y SFTP en :2222):

    docker compose -p rabbitexp up -d
    # en rabbit-test-2: DATABASE_URL=postgres://postgres:password@localhost:5432/postgres?schema=public npx prisma migrate deploy

Arrancar rabbit-test-2 y rabbit-test con esas variables (en Git Bash anteponer
`MSYS_NO_PATHCONV=1`, si no `SFTP_ROOT=/upload` se convierte en ruta de Windows):

    SFTP_HOST=localhost SFTP_PORT=2222 SFTP_USERNAME=nas SFTP_PASSWORD=nas SFTP_ROOT=/upload

Con el publisher en :3000: `node experimento.mjs`
