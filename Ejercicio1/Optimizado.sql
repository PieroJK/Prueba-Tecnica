
DECLARE @ALMACEN   INT  = 8
DECLARE @PDFECINI  DATE = '2025-03-01'
DECLARE @PDFECFIN  DATE = '2025-03-31'

-- [1] CARGA DE ALMACENES EN TABLA TEMPORAL CON ÍNDICE
--     Se agrega índice clustered en N para acelerar el WHILE

SELECT
    ROW_NUMBER() OVER (ORDER BY NALMCOD)            AS N,
    NALMCOD,
    RIGHT('0' + CONVERT(VARCHAR(2), NALMCOD), 2)   AS COD,
    CALMDESCRIPCION
INTO #TAG
FROM LOGALMACEN;

CREATE CLUSTERED INDEX CIX_TAG_N ON #TAG (N);  -- ayuda al lookup dentro del WHILE


-- [2] TABLA DE RESULTADO FINAL — se declara UNA sola vez
--     fuera del loop para acumular todos los almacenes

DECLARE @RESULTADO TABLE
(
    cCtaMadre     VARCHAR(200)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    cCtaContCod   VARCHAR(200)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    cCtaContDesc  VARCHAR(200)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    NINICIO       MONEY,
    NINGRESO      MONEY,
    NSALIDA       MONEY,
    NSALDO        MONEY,
    CPERIODO      VARCHAR(200)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

-- [3] PERIODO ACTUAL — se lee UNA vez antes del loop

DECLARE @sPeriodo     VARCHAR(6);
DECLARE @PeriodoFin   VARCHAR(6) = CONVERT(VARCHAR(6), @PDFECFIN, 112);

SELECT @sPeriodo = RIGHT(nconssisvalor, 4) + SUBSTRING(nconssisvalor, 4, 2)
FROM   constsistema
WHERE  nconssiscod = 113;

-- [4] LOOP POR ALMACÉN

DECLARE @N    INT = 1
DECLARE @NAG  INT;
SELECT @NAG = COUNT(*) FROM #TAG;

WHILE @N <= @NAG
BEGIN

    SELECT @ALMACEN = NALMCOD
    FROM   #TAG
    WHERE  N = @N;

    -- Variables locales del ciclo
    DECLARE @PNALMACEN INT  = @ALMACEN;
    DECLARE @PNPRODTPO INT  = 0;


    -- [5] LÓGICA DE PERIODO: reemplaza el GOTO por IF simple

    IF @PNALMACEN = 0 AND @PeriodoFin = @sPeriodo
        SET @PNALMACEN = 1;
    ELSE IF @PNALMACEN = 1 AND @PeriodoFin = @sPeriodo
        SET @PNALMACEN = 0;

    -- TABLAS DE TRABAJO DEL CICLO
    -- Se crean como #temporales (con índices) en vez de
    -- @variables de tabla, que no admiten estadísticas


    -- [6] SALDOS ANTERIORES
    CREATE TABLE #Saldos
    (
        NALMCOD   INT,
        NALMTPO   INT,
        CBSCOD    VARCHAR(20)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        DSALDO    DATETIME,
        CINICIO   MONEY,
        VINICIO   MONEY,
        LSMOVNRO  VARCHAR(25)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL
    );
    CREATE CLUSTERED INDEX CIX_Saldos_BS ON #Saldos (CBSCOD);


    -- [7] Saldos con último corte antes de fecha inicio
    --     Se precalcula la fecha del día siguiente en una CTE
    --     para no repetir DATEADD en cada columna

    ;WITH UltimoSaldo AS
    (
        SELECT   CBSCOD,
                 MAX(DSALDO) AS DSALDO
        FROM     BSSALDOS WITH (NOLOCK)
        WHERE    DSALDO < @PDFECINI
        GROUP BY CBSCOD
    ),
    SaldoBase AS
    (
        SELECT
            ISNULL(BS.NALMCOD, @PNALMACEN)                    AS NALMCOD,
            ISNULL(BS.NALMTPO, @PNPRODTPO)                    AS NALMTPO,
            S.CBSCOD,
            BS.DSALDO,
            ISNULL(BS.NSTOCK,  0)                              AS CINICIO,
            ISNULL(BS.NMONTO,  0)                              AS VINICIO,
            -- Número de movimiento del día siguiente (formato AAAAMMDD)
            CONVERT(VARCHAR(8),
                DATEADD(DAY, 1, S.DSALDO), 112)               AS LSMOVNRO
        FROM UltimoSaldo S
        LEFT JOIN BSSALDOS BS WITH (NOLOCK)
               ON  BS.CBSCOD  = S.CBSCOD
               AND BS.DSALDO  = S.DSALDO
               AND LEFT(BS.CBSCOD, 3) IN ('111', '112', '113')
               AND BS.DSALDO  < @PDFECINI
               AND BS.NSTOCK  > 0
               AND LEN(BS.CBSCOD) = 11
               AND BS.NALMTPO = @PNPRODTPO
               AND BS.NALMCOD = CASE WHEN @PNALMACEN = -1 THEN BS.NALMCOD ELSE @PNALMACEN END
    )
    INSERT INTO #Saldos
    SELECT * FROM SaldoBase;

    -- Artículos sin saldo previo: se agregan con valores en 0
    INSERT INTO #Saldos (NALMCOD, NALMTPO, CBSCOD, DSALDO, CINICIO, VINICIO, LSMOVNRO)
    SELECT
        0, 0, A.CBSCOD, NULL, 0, 0,
        B.LSMOVNRO
    FROM BienesServicios A WITH (NOLOCK)
    LEFT JOIN #Saldos     B ON A.CBSCOD = B.CBSCOD
    WHERE LEFT(A.CBSCOD, 3) IN ('111', '112', '113')
      AND LEN(A.CBSCOD) = 11
      AND B.CBSCOD IS NULL;


    -- [8] MOVIMIENTOS DEL PERIODO — tabla temporal con índice
    --     Filtro por rango de fecha directo sobre DFECMOV
    --     (evita CONVERT en columna indexable si existe índice)

    CREATE TABLE #Mov
    (
        NMOVNRO    INT            NOT NULL,
        COPECOD    VARCHAR(6)     COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        NMOVESTADO INT,
        NMOVFLAG   INT,
        DFECMOV    DATE,
        CMOVNRO    VARCHAR(25)    COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        CONSTRAINT PK_XMov PRIMARY KEY CLUSTERED (NMOVNRO ASC)
    );

    INSERT INTO #Mov
    SELECT M.NMOVNRO, M.COPECOD, M.NMOVESTADO, M.NMOVFLAG, M.DFECMOV, M.CMOVNRO
    FROM   MOV M WITH (NOLOCK)
    WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN  -- usa la columna date directamente
      AND  M.COPECOD LIKE '591[123]%'
      AND  M.COPECOD NOT IN ('591001', '591021', '591031')
      AND  M.NMOVFLAG   = 0
      AND  M.NMOVESTADO IN (10, 20, 22);

    CREATE INDEX IX_Mov_Ope ON #Mov (COPECOD, NMOVESTADO, NMOVFLAG);

    -- [9] CANTIDADES: ingresos y salidas
    --     Se unifica la lógica con una CTE parametrizada

    CREATE TABLE #Cantidades
    (
        NALMCOD   INT,
        CBSCOD    VARCHAR(20)  COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
        NMOVCANT  MONEY,
        NMONTO    MONEY,
        NTIPO     CHAR(1)      COLLATE SQL_Latin1_General_CP1_CI_AS NULL
    );
    CREATE INDEX IX_Cant_BS ON #Cantidades (CBSCOD, NTIPO, NALMCOD);

    -- ---- INGRESOS (NOPETPO = 1) para PNALMACEN = 0 --------
    IF @PNALMACEN = 0
    BEGIN
        ;WITH Ingresos AS
        (
            -- Ingresos por orden de almacén
            SELECT
                MB.NMOVBSORDEN             AS NALMCOD,
                MB.CBSCOD,
                SUM(MC.NMOVCANT)           AS NCANTIDAD,
                SUM(ABS(MCT.NMOVIMPORTE))  AS VCANTIDAD,
                'I'                        AS NTIPO
            FROM   #Mov M
            INNER JOIN MOVCANT MC  WITH (NOLOCK) ON MC.NMOVNRO  = M.NMOVNRO
            INNER JOIN MOVBS   MB  WITH (NOLOCK) ON MB.NMOVNRO  = M.NMOVNRO
                                                AND MB.NMOVITEM = MC.NMOVITEM
                                                AND MB.NMOVBSORDEN = @PNALMACEN
                                                AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
            INNER JOIN MOVCTA  MCT WITH (NOLOCK) ON MCT.NMOVNRO  = M.NMOVNRO
                                                AND MCT.NMOVITEM = MC.NMOVITEM
            WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
              AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD WITH (NOLOCK)
                                  WHERE NOPETPO = 1 AND NPRODTPO = @PNPRODTPO)
              AND  M.COPECOD NOT IN ('591103')
              AND  M.NMOVESTADO IN (10, 20)
              AND  M.NMOVFLAG = 0
              AND  MB.CBSCOD IN (SELECT CBSCOD FROM #Saldos WHERE CINICIO = 0)
            GROUP BY MB.NMOVBSORDEN, MB.CBSCOD

            UNION ALL

            -- Ingresos por gasto (operación 591120)
            SELECT
                CONVERT(INT, MGA.cDestino),
                MB.CBSCOD,
                SUM(MC.NMOVCANT),
                SUM(ABS(MCT.NMOVIMPORTE)),
                'I'
            FROM   #Mov M
            INNER JOIN MOVCANT  MC  WITH (NOLOCK) ON MC.NMOVNRO  = M.NMOVNRO
            INNER JOIN MOVBS    MB  WITH (NOLOCK) ON MB.NMOVNRO  = M.NMOVNRO
                                                 AND MB.NMOVITEM = MC.NMOVITEM
                                                 AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
            INNER JOIN MovGasto MGA WITH (NOLOCK) ON MGA.nMovNro  = M.NMOVNRO
                                                 AND MGA.cDestino = @PNALMACEN
            INNER JOIN MOVCTA   MCT WITH (NOLOCK) ON MCT.NMOVNRO  = M.NMOVNRO
                                                 AND MCT.NMOVITEM = MC.NMOVITEM
            WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
              AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD WITH (NOLOCK)
                                  WHERE NOPETPO = 1 AND NPRODTPO = @PNPRODTPO)
              AND  M.COPECOD = '591120'
              AND  M.NMOVESTADO = 10
              AND  M.NMOVFLAG = 0
              AND  MB.CBSCOD IN (SELECT CBSCOD FROM #Saldos WHERE CINICIO = 0)
            GROUP BY MGA.cDestino, MB.CBSCOD
        )
        INSERT INTO #Cantidades
        SELECT NALMCOD, CBSCOD, SUM(NCANTIDAD), SUM(VCANTIDAD), NTIPO
        FROM   Ingresos
        GROUP BY NALMCOD, CBSCOD, NTIPO;
    END;

    -- ---- INGRESOS para PNALMACEN > 0 (vía MovGasto / 591103) ----
    IF @PNALMACEN > 0
    BEGIN
        INSERT INTO #Cantidades
        SELECT
            MGA.cDestino,
            MB.CBSCOD,
            ISNULL(SUM(MC.NMOVCANT),          0),
            ISNULL(SUM(ABS(MCT.NMOVIMPORTE)), 0),
            'I'
        FROM   #Mov M
        INNER JOIN MOVCANT  MC  WITH (NOLOCK) ON MC.NMOVNRO  = M.NMOVNRO
        INNER JOIN MOVBS    MB  WITH (NOLOCK) ON MB.NMOVNRO  = M.NMOVNRO
                                             AND MB.NMOVITEM = MC.NMOVITEM
                                             AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
        INNER JOIN MovGasto MGA WITH (NOLOCK) ON MGA.nMovNro  = M.NMOVNRO
                                             AND MGA.cDestino = @PNALMACEN
        INNER JOIN MOVCTA   MCT WITH (NOLOCK) ON MCT.NMOVNRO  = M.NMOVNRO
                                             AND MCT.NMOVITEM = MC.NMOVITEM
        WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
          AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD WITH (NOLOCK)
                              WHERE NOPETPO = 1 AND NPRODTPO = @PNPRODTPO)
          AND  M.COPECOD = '591103'
          AND  M.NMOVESTADO = 10
          AND  M.NMOVFLAG = 0
          AND  MB.CBSCOD IN (SELECT CBSCOD FROM #Saldos WHERE CINICIO = 0)
        GROUP BY MGA.cDestino, MB.CBSCOD;
    END;

    -- ---- SALIDAS (NOPETPO = 2) — artículos sin saldo inicial ----
    INSERT INTO #Cantidades
    SELECT
        MB.NMOVBSORDEN,
        MB.CBSCOD,
        ISNULL(SUM(MC.NMOVCANT),          0),
        ISNULL(SUM(ABS(MCT.NMOVIMPORTE)), 0),
        'S'
    FROM   #Mov M
    INNER JOIN MOVCANT MC  WITH (NOLOCK) ON MC.NMOVNRO  = M.NMOVNRO
    INNER JOIN MOVBS   MB  WITH (NOLOCK) ON MB.NMOVNRO  = M.NMOVNRO
                                        AND MB.NMOVITEM = MC.NMOVITEM
                                        AND MB.NMOVBSORDEN = @PNALMACEN
                                        AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
    INNER JOIN MOVCTA  MCT WITH (NOLOCK) ON MCT.NMOVNRO  = M.NMOVNRO
                                        AND MCT.NMOVITEM = MC.NMOVITEM
    WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
      AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD WITH (NOLOCK)
                          WHERE NOPETPO = 2 AND NPRODTPO = @PNPRODTPO)
      AND  (M.NMOVFLAG = 0 OR (M.COPECOD = '591202'))
      AND  M.NMOVESTADO IN (10, 22)
      AND  MB.CBSCOD IN (SELECT CBSCOD FROM #Saldos WHERE CINICIO = 0)
    GROUP BY MB.NMOVBSORDEN, MB.CBSCOD;

    -- ---- INGRESOS artículos CON saldo (CINICIO > 0) — PNALMACEN = 0 ----
    IF @PNALMACEN = 0
    BEGIN
        ;WITH IngresosConSaldo AS
        (
            SELECT
                MB.NMOVBSORDEN         AS NALMCOD,
                MB.CBSCOD,
                SUM(MC.NMOVCANT)       AS NCANTIDAD,
                SUM(ABS(MCT.NMOVIMPORTE)) AS VCANTIDAD,
                'I'                    AS NTIPO
            FROM   #Mov M
            INNER JOIN MOVCANT MC  ON MC.NMOVNRO  = M.NMOVNRO
            INNER JOIN MOVBS   MB  ON MB.NMOVNRO  = M.NMOVNRO
                                  AND MB.NMOVITEM = MC.NMOVITEM
                                  AND MB.NMOVBSORDEN = @PNALMACEN
                                  AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
            INNER JOIN MOVCTA  MCT ON MCT.NMOVNRO  = M.NMOVNRO
                                  AND MCT.NMOVITEM = MC.NMOVITEM
            INNER JOIN #Saldos  S  ON S.CBSCOD = MB.CBSCOD
                                  AND S.CINICIO > 0
                                  AND M.CMOVNRO > S.LSMOVNRO  -- reemplaza subconsulta correlacionada
            WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
              AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD
                                  WHERE NOPETPO = 1 AND NPRODTPO = @PNPRODTPO)
              AND  M.COPECOD NOT IN ('591103')
              AND  M.NMOVESTADO IN (10, 20)
              AND  M.NMOVFLAG = 0
            GROUP BY MB.NMOVBSORDEN, MB.CBSCOD

            UNION ALL

            SELECT
                CONVERT(INT, MGA.cDestino),
                MB.CBSCOD,
                SUM(MC.NMOVCANT),
                SUM(ABS(MCT.NMOVIMPORTE)),
                'I'
            FROM   #Mov M
            INNER JOIN MOVCANT  MC  WITH (NOLOCK) ON MC.NMOVNRO  = M.NMOVNRO
            INNER JOIN MOVBS    MB  WITH (NOLOCK) ON MB.NMOVNRO  = M.NMOVNRO
                                                 AND MB.NMOVITEM = MC.NMOVITEM
                                                 AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
            INNER JOIN MovGasto MGA WITH (NOLOCK) ON MGA.nMovNro  = M.NMOVNRO
                                                 AND CONVERT(INT, MGA.cDestino) = @PNALMACEN
            INNER JOIN MOVCTA   MCT WITH (NOLOCK) ON MCT.NMOVNRO  = M.NMOVNRO
                                                 AND MCT.NMOVITEM = MC.NMOVITEM
            INNER JOIN #Saldos   S  ON S.CBSCOD = MB.CBSCOD
                                   AND S.CINICIO > 0
                                   AND M.CMOVNRO > S.LSMOVNRO
            WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
              AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD WITH (NOLOCK)
                                  WHERE NOPETPO = 1 AND NPRODTPO = @PNPRODTPO)
              AND  M.COPECOD = '591120'
              AND  M.NMOVESTADO = 10
              AND  M.NMOVFLAG = 0
            GROUP BY MGA.cDestino, MB.CBSCOD
        )
        INSERT INTO #Cantidades
        SELECT NALMCOD, CBSCOD, SUM(NCANTIDAD), SUM(VCANTIDAD), NTIPO
        FROM   IngresosConSaldo
        GROUP BY NALMCOD, CBSCOD, NTIPO;
    END;

    IF @PNALMACEN > 0
    BEGIN
        INSERT INTO #Cantidades
        SELECT
            MGA.cDestino,
            MB.CBSCOD,
            ISNULL(SUM(MC.NMOVCANT),          0),
            ISNULL(SUM(ABS(MCT.NMOVIMPORTE)), 0),
            'I'
        FROM   #Mov M
        INNER JOIN MOVCANT  MC  WITH (NOLOCK) ON MC.NMOVNRO  = M.NMOVNRO
        INNER JOIN MOVBS    MB  WITH (NOLOCK) ON MB.NMOVNRO  = M.NMOVNRO
                                             AND MB.NMOVITEM = MC.NMOVITEM
                                             AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
        INNER JOIN MovGasto MGA WITH (NOLOCK) ON MGA.nMovNro  = M.NMOVNRO
                                             AND CONVERT(INT, MGA.cDestino) = @PNALMACEN
        INNER JOIN MOVCTA   MCT WITH (NOLOCK) ON MCT.NMOVNRO  = M.NMOVNRO
                                             AND MCT.NMOVITEM = MC.NMOVITEM
        INNER JOIN #Saldos   S  ON S.CBSCOD = MB.CBSCOD
                               AND S.CINICIO > 0
                               AND M.CMOVNRO > S.LSMOVNRO
        WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
          AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD WITH (NOLOCK)
                              WHERE NOPETPO = 1 AND NPRODTPO = @PNPRODTPO)
          AND  M.COPECOD = '591103'
          AND  M.NMOVESTADO = 10
          AND  M.NMOVFLAG = 0
        GROUP BY MGA.cDestino, MB.CBSCOD;
    END;

    -- SALIDAS artículos CON saldo (CINICIO > 0)
    INSERT INTO #Cantidades
    SELECT
        MB.NMOVBSORDEN,
        MB.CBSCOD,
        ISNULL(SUM(MC.NMOVCANT),          0),
        ISNULL(SUM(ABS(MCT.NMOVIMPORTE)), 0),
        'S'
    FROM   #Mov M
    INNER JOIN MOVCANT MC  ON MC.NMOVNRO  = M.NMOVNRO
    INNER JOIN MOVBS   MB  ON MB.NMOVNRO  = M.NMOVNRO
                          AND MB.NMOVITEM = MC.NMOVITEM
                          AND MB.NMOVBSORDEN = CASE WHEN @PNALMACEN = -1 THEN MB.NMOVBSORDEN ELSE @PNALMACEN END
                          AND LEFT(MB.CBSCOD, 3) IN ('111', '112', '113')
    INNER JOIN MOVCTA  MCT ON MCT.NMOVNRO  = M.NMOVNRO
                          AND MCT.NMOVITEM = MC.NMOVITEM
    INNER JOIN #Saldos  S  ON S.CBSCOD = MB.CBSCOD
                          AND S.CINICIO > 0
                          AND M.CMOVNRO > S.LSMOVNRO
    WHERE  M.DFECMOV BETWEEN @PDFECINI AND @PDFECFIN
      AND  M.COPECOD IN (SELECT COPECOD FROM LOGOPEPROD
                          WHERE NOPETPO = 2 AND NPRODTPO = @PNPRODTPO)
      AND  M.NMOVFLAG NOT IN (1, 2, 3)
      AND  M.NMOVESTADO IN (10, 22)
    GROUP BY MB.NMOVBSORDEN, MB.CBSCOD;

    -- Eliminar filas en cero (sin movimiento real)
    DELETE FROM #Cantidades WHERE NMOVCANT = 0 AND NMONTO = 0;

    -- [10] LISTADO CONSOLIDADO — CTE reemplaza 3 INSERTs repetidos

    ;WITH CTE_Listado AS
    (
        -- Artículos con ingreso
        SELECT
            C.NALMCOD,
            C.CBSCOD,
            ISNULL(S.CINICIO,  0)      AS CINICIO,
            ISNULL(C.NMOVCANT, 0)      AS CINGRESO,
            ISNULL(CS.NMOVCANT,0)      AS CSALIDA,
            ISNULL(S.VINICIO,  0)      AS VINICIO,
            ISNULL(C.NMONTO,   0)      AS VINGRESO,
            ISNULL(CS.NMONTO,  0)      AS VSALIDA
        FROM  #Cantidades C
        LEFT JOIN (SELECT CBSCOD, NALMCOD,
                          SUM(CINICIO) AS CINICIO,
                          SUM(VINICIO) AS VINICIO
                   FROM   #Saldos
                   GROUP BY CBSCOD, NALMCOD)         S  ON S.CBSCOD = C.CBSCOD AND S.NALMCOD = C.NALMCOD
        LEFT JOIN (SELECT CBSCOD, NALMCOD,
                          SUM(NMOVCANT) AS NMOVCANT,
                          SUM(NMONTO)   AS NMONTO
                   FROM   #Cantidades
                   WHERE  NTIPO = 'S'
                   GROUP BY CBSCOD, NALMCOD)        CS  ON CS.CBSCOD = C.CBSCOD AND CS.NALMCOD = C.NALMCOD
        WHERE  C.NTIPO = 'I'
          AND  C.CBSCOD  IN (SELECT CBSCOD FROM #Saldos)
          AND  C.NALMCOD = @PNALMACEN

        UNION ALL

        -- Artículos con solo salida y con saldo inicial
        SELECT
            C.NALMCOD,
            C.CBSCOD,
            ISNULL(S.CINICIO,  0),
            ISNULL(CI.NMOVCANT,0),
            ISNULL(C.NMOVCANT, 0),
            ISNULL(S.VINICIO,  0),
            ISNULL(CI.NMONTO,  0),
            ISNULL(C.NMONTO,   0)
        FROM  #Cantidades C
        LEFT JOIN (SELECT CBSCOD, NALMCOD,
                          SUM(CINICIO) AS CINICIO,
                          SUM(VINICIO) AS VINICIO
                   FROM   #Saldos
                   GROUP BY CBSCOD, NALMCOD)         S  ON S.CBSCOD = C.CBSCOD AND S.NALMCOD = C.NALMCOD
        LEFT JOIN (SELECT CBSCOD, NALMCOD,
                          SUM(NMOVCANT) AS NMOVCANT,
                          SUM(NMONTO)   AS NMONTO
                   FROM   #Cantidades
                   WHERE  NTIPO = 'I'
                   GROUP BY CBSCOD, NALMCOD)        CI  ON CI.CBSCOD = C.CBSCOD AND CI.NALMCOD = C.NALMCOD
        WHERE  C.NTIPO   = 'S'
          AND  C.CBSCOD  IN (SELECT CBSCOD FROM #Saldos)
          AND  C.NALMCOD = @PNALMACEN
          AND  ISNULL(S.CINICIO,  0) > 0
          AND  ISNULL(CI.NMOVCANT,0) = 0

        UNION ALL

        -- Artículos solo con saldo (sin movimientos en el periodo)
        SELECT
            S.NALMCOD, S.CBSCOD,
            S.CINICIO, 0, 0,
            S.VINICIO, 0, 0
        FROM  #Saldos S
        WHERE S.CBSCOD NOT IN (SELECT CBSCOD FROM #Cantidades)
          AND S.NALMCOD IS NOT NULL
    ),
    CTE_Almacenes AS
    (
        SELECT
            L.CBSCOD,
            B.CBSDESCRIPCION,
            L.CINICIO,
            L.CINGRESO,
            L.CSALIDA,
            (L.CINICIO + L.CINGRESO - L.CSALIDA)        AS NSALDO,
            L.VINICIO,
            L.VINGRESO,
            L.VSALIDA,
            ROUND(L.VINICIO + L.VINGRESO - L.VSALIDA, 2) AS VSALDO,
            L.NALMCOD
        FROM  CTE_Listado L
        INNER JOIN BIENESSERVICIOS B ON B.CBSCOD = L.CBSCOD
        WHERE (L.CINICIO > 0 OR L.CINGRESO > 0 OR L.CSALIDA > 0)

        UNION ALL

        -- Artículos con saldo 0 que no aparecen en el listado
        SELECT
            B.CBSCOD, B.CBSDESCRIPCION,
            0, 0, 0, 0, 0, 0, 0, 0,
            S.NALMCOD
        FROM   #Saldos S
        INNER JOIN BIENESSERVICIOS B ON B.CBSCOD = S.CBSCOD
        WHERE  S.CINICIO = 0
          AND  LEN(S.CBSCOD) = 11
          AND  LEFT(S.CBSCOD, 3) IN ('111', '112', '113')
          AND  S.CBSCOD NOT IN (SELECT CBSCOD FROM CTE_Listado
                                 WHERE CINICIO > 0 OR CINGRESO > 0 OR CSALIDA > 0)
    )
    SELECT *
    INTO   #TK
    FROM   CTE_Almacenes
    ORDER BY CBSCOD;

    -- [11] MAPEO DE CUENTAS CONTABLES

    DECLARE @AlmCod VARCHAR(2) = RIGHT('00' + CONVERT(VARCHAR(2),
                CASE WHEN @ALMACEN = 0 THEN 34 ELSE @ALMACEN END), 2);

    SET LANGUAGE spanish;

    SELECT
        REPLACE(
            COALESCE(B.cCtaContCod, C.cCtaContCod, D.cCtaContCod,
                     E.cCtaContCod, F.cCtaContCod, G.cCtaContCod),
            'AG', ''
        ) + @AlmCod                         AS cCtaContCod,
        A.*
    INTO #TC
    FROM #TK A
    LEFT JOIN (SELECT * FROM CtaBS WHERE cOpeCod = 591205 AND LEN(cObjetoCod) = 5) B ON LEFT(A.CBSCOD, 5) = B.cObjetoCod
    LEFT JOIN (SELECT * FROM CtaBS WHERE cOpeCod = 591205 AND LEN(cObjetoCod) = 8) C ON LEFT(A.CBSCOD, 8) = C.cObjetoCod
    LEFT JOIN (SELECT * FROM CtaBS WHERE cOpeCod = 591101 AND LEN(cObjetoCod) = 5) D ON LEFT(A.CBSCOD, 5) = D.cObjetoCod
    LEFT JOIN (SELECT * FROM CtaBS WHERE cOpeCod = 591110 AND LEN(cObjetoCod) = 8) E ON LEFT(A.CBSCOD, 8) = E.cObjetoCod
    LEFT JOIN (SELECT * FROM CtaBS WHERE cOpeCod = 501207 AND LEN(cObjetoCod) = 5) F ON LEFT(A.CBSCOD, 5) = F.cObjetoCod
    LEFT JOIN (SELECT * FROM CtaBS WHERE cOpeCod = 501205 AND LEN(cObjetoCod) = 5) G ON LEFT(A.CBSCOD, 5) = G.cObjetoCod
    WHERE (NINICIO > 0 OR NINGRESO > 0 OR NSALIDA > 0);

    -- Corrección específica de cuenta
    UPDATE #TC
    SET    cCtaContCod = '19110712' + @AlmCod
    WHERE  CBSCOD = '11104051001';


    -- [12] ACUMULA RESULTADO EN TABLA FINAL
    
    INSERT INTO @RESULTADO
    SELECT
        UPPER(C.cCtaContDesc)                                    AS cCtaMadre,
        A.cCtaContCod,
        UPPER(B.cCtaContDesc)                                    AS cCtaContDesc,
        SUM(A.VINICIO)                                           AS NINICIO,
        SUM(A.VINGRESO)                                          AS NINGRESO,
        SUM(A.VSALIDA)                                           AS NSALIDA,
        SUM(A.VINICIO + A.VINGRESO - A.VSALIDA)                 AS NSALDO,
        UPPER(DATENAME(MONTH, @PDFECFIN) + ' ' +
              CAST(DATEPART(YEAR, @PDFECFIN) AS VARCHAR(4)))     AS CPERIODO
    FROM   #TC A
    INNER JOIN CtaCont B ON B.cCtaContCod = A.cCtaContCod
    LEFT  JOIN CtaCont C ON C.cCtaContCod = LEFT(A.cCtaContCod, LEN(A.cCtaContCod) - 2)
    GROUP BY A.cCtaContCod, B.cCtaContDesc, C.cCtaContDesc
    ORDER BY A.cCtaContCod ASC;


    -- [13] LIMPIEZA DE TEMPORALES DEL CICLO

    DROP TABLE IF EXISTS #TK;
    DROP TABLE IF EXISTS #TC;
    DROP TABLE IF EXISTS #Mov;
    DROP TABLE IF EXISTS #Saldos;
    DROP TABLE IF EXISTS #Cantidades;

    SET @N = @N + 1;

END; -- FIN WHILE

DROP TABLE IF EXISTS #TAG;


--  RESULTADO 

SELECT *
FROM   @RESULTADO
ORDER BY cCtaContCod ASC, cCtaMadre ASC;