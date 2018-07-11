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
	SchemaDb2 VARCHAR (MAX) NOT NULL,
	SchemaDb2Exists VARCHAR (MAX),
	TableDb2 VARCHAR (MAX) NOT NULL,
	TableDb2Exists VARCHAR (MAX),
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

-- Compara la Table y Schema que recibe contra los de la otra DB.
CREATE PROCEDURE SPCompareTables @Db1 VARCHAR (MAX), @Db2 VARCHAR (MAX), @AnId NUMERIC (18, 0), @Db1Schema VARCHAR (MAX), @Db1Table VARCHAR (MAX) AS
	BEGIN
		BEGIN TRY
			/*
			-- Se deja comentado el manejo de transacciones en caso de ser necesario.
			BEGIN TRANSACTION
			*/
			SET NOCOUNT ON
			DECLARE @Statement NVARCHAR (MAX)
			-- Busca la Table y el Schema en la segunda DB.
			DECLARE @Cantidad INT,
				@TableExists INT,
				@SchemaExists INT
			SET @Statement = 'SELECT @TableExists = COUNT (*)
								FROM ' + @Db2 +'.INFORMATION_SCHEMA.TABLES
								WHERE TABLE_NAME = ''' + @Db1Table + ''''
			EXECUTE SP_EXECUTESQL @Statement, N'@TableExists INT OUTPUT', @TableExists = @Cantidad OUTPUT
			IF (@Cantidad = 0)
				BEGIN
					INSERT INTO AnalisisTablas (AnalisisDbsId, SchemaDb2, TableDb2, SchemaDb2Exists, TableDb2Exists)
						VALUES (@AnId, @Db1Schema, @Db1Table, 'No', 'No se comprueba')
					RAISERROR ('ERROR! No existe la Tabla en la segunda Base de Datos.',15 ,1) 
				END
			SET @Statement = 'SELECT @SchemaExists = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLES
								WHERE TABLE_SCHEMA = ''' + @Db1Schema + ''''
			EXECUTE SP_EXECUTESQL @Statement, N'@SchemaExists INT OUTPUT', @SchemaExists = @Cantidad OUTPUT
			IF (@Cantidad = 0)
				BEGIN
					INSERT INTO AnalisisTablas (AnalisisDbsId, SchemaDb2, TableDb2, SchemaDb2Exists, TableDb2Exists)
						VALUES (@AnId, @Db1Schema, @Db1Table, 'Si', 'No')
					RAISERROR ('ERROR! No existe el Esquema en la segunda Base de Datos.',15 ,1)
				END
			-- Control de PKs.
			DECLARE @CantPkDb1 INT,
				@CantPkDb2 INT,
				@Pk INT,
				@PkDb1 VARCHAR (MAX),
				@PkDb2 VARCHAR (MAX)
			SET @Statement = 'SELECT @Pk = COUNT (*)
								FROM ' + @Db1 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = "PRIMARY KEY"
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Pk INT OUTPUT', @Pk = @CantPkDb1 OUTPUT
			SET @Statement = 'SELECT @Pk = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db2 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = "PRIMARY KEY"
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Pk INT OUTPUT', @Pk = @CantPkDb2 OUTPUT
			IF (@CantPkDb1 = 1)
				BEGIN
					SET @PkDb1 = 'Utiliza Primary Key simple.'
				END
			ELSE
				BEGIN
					IF (@CantPkDb1 >= 2)
						BEGIN
							SET @PkDb1 = 'Utiliza Primary Key compuesta.'
						END
					ELSE
						BEGIN
							SET @PkDb1 = 'No utiliza Primary Key.'
						END
				END
			IF (@CantPkDb2 = 1)
				BEGIN
					SET @PkDb2 = 'Utiliza Primary Key simple.'
				END
			ELSE
				BEGIN
					IF (@CantPkDb2 >= 2)
						BEGIN
							SET @PkDb2 = 'Utiliza Primary Key compuesta.'
						END
					ELSE
						BEGIN
							SET @PkDb2 = 'No utiliza Primary Key.'
						END
				END
			-- Control de FKs.
			-- Para seguir desarrollando...
			/*
			-- Se realiza un Commit cuando llega sin errores.
			COMMIT TRANSACTION
			*/
		END TRY
		BEGIN CATCH
			PRINT 'Se ha producido un error, revisar el log de errores.'
			INSERT INTO errorLog (AnalisisId, ErrorNumber, ErrorMessage, ErrorLine, ErrorSeverity, ErrorState, ErrorProcedure, FechaHora, Usuario)
				VALUES (@AnId, ERROR_NUMBER (), ERROR_MESSAGE (), ERROR_LINE (), ERROR_SEVERITY (), ERROR_STATE (), ERROR_PROCEDURE (), GETDATE (), SYSTEM_USER)
			/*
			-- Una vez manejados los errores, se realiza un control previo a Rollback o Commit.
			IF (XACT_STATE() = -1)
				BEGIN
					PRINT 'La transacción no puede ser efectuada. Haciendo Rollback...'
					ROLLBACK TRANSACTION
				END
			IF (XACT_STATE() = 1)
				BEGIN
					PRINT 'La transacción puede ser efectuada a pesar de los errores. Realizando Commit...'
					COMMIT TRANSACTION
				END
			*/
		END CATCH
	END
GO

-- Chequea que existan ambas DBs con manejo de errores, y llama al SP que va a comparar Tables.
CREATE PROCEDURE SPCompareDbs @Db1 VARCHAR (MAX), @Db2 VARCHAR (MAX) AS
	BEGIN
		BEGIN TRY
			SET NOCOUNT ON
			DECLARE @AnId NUMERIC (18, 0)
			IF (DB_ID (@Db1) IS NOT NULL)
				BEGIN
					IF (DB_ID (@Db2) IS NOT NULL)
						BEGIN
							INSERT INTO AnalisisDbs (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'Si', 'Si')
								SET @AnId = @@IDENTITY
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
							INSERT INTO AnalisisDbs (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'No', 'Si')
							RAISERROR ('ERROR! No existe la primer Base de Datos dada.', 16, 1)
						END
					ELSE
						BEGIN
							INSERT INTO AnalisisDbs (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'No', 'No')
							RAISERROR ('ERROR! No existen las Bases de Datos dadas.', 16, 1)
						END
				END
			DECLARE @Statement NVARCHAR (MAX),
				@Db1Schema VARCHAR (MAX),
				@Db1Table VARCHAR (MAX)
			SET @Statement = 'DECLARE CompareCursor CURSOR FOR
									SELECT S.name AS SchemaName, T.name AS TableName
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
-- End.


