/*
	Grupo ?
	Andreoli, Rodrigo Emanuel - 38.425.148


*/

/*	***** Creación de la DB *****	*/

-- Empieza asegurando que la DB no exista.
USE Master
GO

IF EXISTS (
	SELECT 1 
	FROM sys.databases 
	WHERE name = 'TpIntegrador'
	)
	BEGIN
		DROP DATABASE TpIntegrador
	END
GO

-- Crea la DB.
CREATE DATABASE TpIntegrador
GO

USE TpIntegrador
GO

/*	***** Creación de las tablas *****	*/

CREATE TABLE AnalisisDbs (
	Id NUMERIC (18, 0) IDENTITY (1, 1) NOT NULL,
	Db1 VARCHAR (MAX) NOT NULL,
	Db2 VARCHAR (MAX) NOT NULL,
	Db1Exists VARCHAR (MAX) NOT NULL,
	Db2Exists VARCHAR (MAX) NOT NULL,
	PRIMARY KEY (Id)
)
GO

CREATE TABLE AnalisisTablas (
	Id NUMERIC (18, 0) IDENTITY (1, 1) NOT NULL,
	AnalisisDbsId NUMERIC (18, 0) NOT NULL,
	EsquemaDb1 VARCHAR (MAX) NOT NULL,
	EsquemaDb2 VARCHAR (MAX),
	TablaDb1 VARCHAR (MAX) NOT NULL,
	TablaDb2 VARCHAR (MAX),
	PkDb1 VARCHAR (MAX),
	PkDb2 VARCHAR (MAX),
	FkDb1 VARCHAR (MAX),
	FkDb2 VARCHAR (MAX),
	UniqueDb1 VARCHAR (MAX),
	UniqueDb2 VARCHAR (MAX),
	CheckDb1 VARCHAR (MAX),
	CheckDb2 VARCHAR (MAX),
	PRIMARY KEY (Id),
	CONSTRAINT fk_AnalisisDbs_Id FOREIGN KEY (AnalisisDbsID) 
		REFERENCES AnalisisDbs (Id)
)
GO

CREATE TABLE AnalisisColumnas (
	Id NUMERIC (18, 0) IDENTITY (1, 1) NOT NULL,
	AnalisisTablasId NUMERIC (18, 0) NOT NULL,
	Autoincremental VARCHAR(MAX),
	NombreColumna VARCHAR(MAX),
	Posicion VARCHAR(MAX),
	DefaultColumna VARCHAR(MAX),
	TipoDato VARCHAR(MAX),
	Tamaño VARCHAR(MAX),
	TipoUsuario VARCHAR(MAX),
	PRIMARY KEY (Id),
	CONSTRAINT fk_AnalisisTablas_Id FOREIGN KEY (AnalisisTablasId) 
		REFERENCES AnalisisTablas (Id)
)
GO

CREATE TABLE errorLog (
	Id NUMERIC (18, 0) IDENTITY (1, 1) NOT NULL,
	AnalisisId NUMERIC (18, 0),
	ErrorNumber int NULL,
	ErrorMessage NVARCHAR (2048) NULL,
	ErrorLine int NULL,
	ErrorSeverity int NULL,
	ErrorState int NULL,
	ErrorProcedure NVARCHAR (126) NULL,
	FechaHora DATETIME NULL,
	Usuario VARCHAR(255) NULL,
	PRIMARY KEY (Id)
)
GO

/*	***** Creación de los procedimientos y funciones *****	*/

-- Compara que las Tables de las DBs sean las mismas?
CREATE PROCEDURE SPCompareTables @Db1 VARCHAR (MAX), @Db2 VARCHAR (MAX), @AnId NUMERIC (18, 0), @Db1Schema VARCHAR (MAX), @Db1Table VARCHAR (MAX) AS
	BEGIN
		PRINT '.' -- Hasta acá llegué
	END
GO

-- Chequea que existan ambas DBs con manejo de errores, y llama al SP que va a comparar Tables.
CREATE PROCEDURE SPCompareDbs @Db1 VARCHAR (MAX), @Db2 VARCHAR (MAX) AS
	BEGIN
		BEGIN TRY
			SET NOCOUNT ON
			DECLARE @AnId NUMERIC (18, 0) = @@IDENTITY
			IF (DB_ID (@Db1) IS NOT NULL)
				BEGIN
					IF (DB_ID (@Db2) IS NOT NULL)
						BEGIN
							INSERT INTO AnalisisDbs (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'Si', 'Si')
						END
					ELSE
						BEGIN
							INSERT INTO AnalisisDbs (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'Si', 'No')
							RAISERROR ('ERROR! No existe la segunda Base de Datos dada.', 16, 1)
						END
				END
			ELSE
				BEGIN
					IF (DB_ID (@Db2) IS NOT NULL)
						BEGIN
							INSERT INTO Analisis (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'No', 'Si')
							RAISERROR ('ERROR! No existe la primer Base de Datos dada.', 16, 1)
						END
					ELSE
						BEGIN
							INSERT INTO Analisis (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'No', 'No')
							RAISERROR ('ERROR! No existen las Bases de Datos dadas.', 16, 1)
						END
				END
			DECLARE @Statement NVARCHAR (MAX),
				@Db1Schema VARCHAR (MAX),
				@Db1Table VARCHAR (MAX)
			SET @Statement = 'DECLARE CompareCursor CURSOR FOR
									SELECT S.name AS Schema, T.name AS Table
										FROM ' + @Db1 + '.sys.schemas AS S
										JOIN ' + @Db1 + '.sys.tables AS T ON S.schema_id = T.schema_id'
			EXECUTE SP_EXECUTESQL @Statement
			OPEN CompareCursor
			FETCH NEXT FROM CompareCursor
				INTO @Db1Schema, @Db1Table
			WHILE (@@FETCH_STATUS = 0)
				BEGIN
					EXECUTE SPCompareTables @Db1, @Db2, @AnId, @Db1Schema, @Db1Table
					FETCH NEXT FROM CompareCursor
						INTO @Db1Schema, @Db1Table
				END
			CLOSE CompareCursor
			DEALLOCATE CompareCursor
		END TRY
		BEGIN CATCH
			PRINT 'Se ha producido un error, revisar el log de errores.'
			INSERT INTO errorLog (AnalisisId, ErrorNumber, ErrorMessage, ErrorLine, ErrorSeverity, ErrorState, ErrorProcedure, FechaHora, Usuario)
				VALUES (@AnId, ERROR_NUMBER (), ERROR_MESSAGE (), ERROR_LINE (), ERROR_SEVERITY (), ERROR_STATE (), ERROR_PROCEDURE (), GETDATE (), SYSTEM_USER)
			END CATCH
	END
GO

EXECUTE SPCompareDbs bd1, bd3

select * from errorLog