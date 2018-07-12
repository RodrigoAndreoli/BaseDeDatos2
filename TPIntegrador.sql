/*
	Grupo ?
	Andreoli	Rodrigo Emanuel		38.425.148
	Bertoli		Pablo Alejandro		36.154.148

*/

/*	***** Creación de la DB *****	*/

-- Confirma que la DB no exista.
USE master
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
	Tamanio VARCHAR(MAX),
	PRIMARY KEY (Id),
	CONSTRAINT fk_AnalisisTablas_Id FOREIGN KEY (AnalisisTablasId) 
		REFERENCES AnalisisTablas (Id)
)
GO

CREATE TABLE ErrorLog (
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

/*
CREATE TABLE AlterScript (
	Id NUMERIC (18, 0) IDENTITY (1, 1) NOT NULL,
	AnalisisId NUMERIC (18, 0),
	PRIMARY KEY (Id)
)
GO
*/

/*	***** Creación de los procedimientos y funciones *****	*/

-- Compara la informacion de la Column que recibe contra las de la Table de la otra DB.
CREATE PROCEDURE SPCompareColumns @Db1 VARCHAR (MAX), 
									@Db2 VARCHAR (MAX), 
									@AnTableId NUMERIC (18, 0), 
									@Db1Schema VARCHAR (MAX), 
									@Db1Table VARCHAR (MAX), 
									@IsIdentity INT , 
									@ColumnName  VARCHAR (MAX), 
									@Position INT, 
									@ColumnDefault VARCHAR (MAX), 
									@DataType  VARCHAR (MAX), 
									@MaxLength INT AS
	BEGIN			
		BEGIN TRY
			/*
			-- Se deja comentado el manejo de transacciones en caso de ser necesario.
			BEGIN TRANSACTION
			*/
			SET NOCOUNT ON
			DECLARE	@Statement NVARCHAR (MAX),
				@Cantidad INT,
				@ColumnExists INT,
				@Identity NVARCHAR(MAX),
				@Posicion NVARCHAR(MAX), 
				@DefaultColumna NVARCHAR(MAX), 
				@TipoDato NVARCHAR(MAX), 
				@Tamanio NVARCHAR(MAX)
			
			-- Busca la Table y la columna en la segunda DB.
			SET @Statement = 'SELECT @ColumnExists = COUNT (*) 
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.COLUMNS
								WHERE TABLE_NAME = ''' + @Db1Table + '''
									AND COLUMN_NAME = ''' + @ColumnName + ''''
			EXECUTE SP_EXECUTESQL @Statement, N'@ColumnExists INT OUTPUT', @ColumnExists = @Cantidad OUTPUT
			IF (@Cantidad = 0)
				BEGIN
					DECLARE @Mensaje VARCHAR (MAX) = 'ERROR! No existe la Columna ' + @ColumnName + ' en base de datos destino'
					RAISERROR (@Mensaje ,15 ,1)
				END
			ELSE
				BEGIN
					DECLARE @IsIdentity2 INT ,
						@Position2 INT, 
						@ColumnDefault2 VARCHAR (MAX), 
						@DataType2  VARCHAR (MAX), 
						@MaxLength2 INT,
						@IsIdentitySql INT ,
						@PositionSql INT, 
						@ColumnDefaultSql VARCHAR (MAX), 
						@DataTypeSql  VARCHAR (MAX), 
						@MaxLengthSql INT
					
					-- Chequeo de identity
					SET @Statement = 'SELECT DISTINCT @IsIdentitySql = C.is_identity
										FROM ' + @Db2 + '.SYS.COLUMNS AS C
											JOIN ' + @Db2 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
										WHERE I.TABLE_NAME = ''' + @Db1Table + '''
											AND I.TABLE_SCHEMA = ''' + @Db1Schema + '''
											AND I.COLUMN_NAME = ''' + @ColumnName + ''''
					EXECUTE SP_EXECUTESQL @Statement, N'@IsIdentitySql INT OUTPUT', @IsIdentitySql = @IsIdentity2 OUTPUT
					IF (@IsIdentity = @IsIdentity2)
						BEGIN
							SET @Identity = 'Coincide'
						END
					ELSE
						BEGIN
							SET @Identity = 'Diefere'
						END
					
					-- Chequeo de position
					SET @Statement = 'SELECT DISTINCT @PositionSql = I.ORDINAL_POSITION
										FROM ' + @Db2 + '.SYS.COLUMNS AS C
											JOIN ' + @Db2 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
										WHERE I.TABLE_NAME = ''' + @Db1Table + '''
											AND I.TABLE_SCHEMA = ''' + @Db1Schema + '''
											AND I.COLUMN_NAME = ''' + @ColumnName + ''''
					EXECUTE SP_EXECUTESQL @Statement, N'@PositionSql INT OUTPUT', @PositionSql = @Position2 OUTPUT
					IF (@Position = @Position2)
						BEGIN
							SET @Posicion = 'Coincide'
						END
					ELSE
						BEGIN
							SET @Posicion = 'Difiere'
						END

					-- Chequeo de Default Columna
					SET @Statement = 'SELECT DISTINCT @ColumnDefaultSql = I.COLUMN_DEFAULT
										FROM ' + @Db2 + '.SYS.COLUMNS AS C
											JOIN ' + @Db2 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
										WHERE I.TABLE_NAME = ''' + @Db1Table + '''
											AND I.TABLE_SCHEMA = ''' + @Db1Schema + '''
											AND I.COLUMN_NAME = ''' + @ColumnName + ''''
					EXECUTE SP_EXECUTESQL @Statement, N'@ColumnDefaultSql VARCHAR(MAX) OUTPUT', @ColumnDefaultSql = @ColumnDefault2 OUTPUT
					IF ((@ColumnDefault = @ColumnDefault2) OR ((@ColumnDefault IS NULL) AND (@ColumnDefault2 IS NULL)))
						BEGIN
							SET @DefaultColumna = 'Coincide'
						END
					ELSE
						BEGIN
							SET @DefaultColumna = 'Difiere'
						END

					-- Chequeo de Tipo de dato
					SET @Statement = 'SELECT DISTINCT @DataTypeSql = I.DATA_TYPE
										FROM ' + @Db2 + '.SYS.COLUMNS AS C
											JOIN ' + @Db2 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
										WHERE I.TABLE_NAME = ''' + @Db1Table + '''
											AND I.TABLE_SCHEMA = ''' + @Db1Schema + '''
											AND I.COLUMN_NAME = ''' + @ColumnName + ''''
					EXECUTE SP_EXECUTESQL @Statement, N'@DataTypeSql VARCHAR(MAX) OUTPUT', @DataTypeSql = @DataType2 OUTPUT
					IF ((@DataType = @DataType2) OR ((@DataType IS NULL) AND (@DataType2 IS NULL)))
						BEGIN
							SET @TipoDato = 'Coincide'
						END
					ELSE
						BEGIN
							SET @TipoDato = 'Difiere'
						END
					
					-- Chequeo de Maxima Longitud
					SET @Statement = 'SELECT DISTINCT @MaxLengthSql = C.max_length
										FROM ' + @Db2 + '.SYS.COLUMNS AS C
											JOIN ' + @Db2 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
										WHERE I.TABLE_NAME = ''' + @Db1Table + '''
											AND I.TABLE_SCHEMA = ''' + @Db1Schema + '''
											AND I.COLUMN_NAME = ''' + @ColumnName + ''''
					EXECUTE SP_EXECUTESQL @Statement, N'@MaxLengthSql INT OUTPUT', @MaxLengthSql = @MaxLength2 OUTPUT
					IF (@MaxLength = @MaxLength2)
						BEGIN
							SET @Tamanio = 'Coincide'
						END
					ELSE
						BEGIN
							SET @Tamanio = 'Difiere'
						END
				END
								
			INSERT INTO AnalisisColumnas (AnalisisTablasId ,Autoincremental ,NombreColumna ,Posicion ,DefaultColumna ,TipoDato ,Tamanio)
				VALUES (@AnTableId, @Identity, @ColumnName, @Posicion, @DefaultColumna, @TipoDato, @Tamanio)

			/*
			-- Se realiza un Commit cuando llega sin errores.
			COMMIT TRANSACTION
			*/
		END TRY
		BEGIN CATCH
			PRINT 'Se ha producido un error, revisar el log de errores.'
			INSERT INTO ErrorLog (AnalisisId, ErrorNumber, ErrorMessage, ErrorLine, ErrorSeverity, ErrorState, ErrorProcedure, FechaHora, Usuario)
				VALUES (@AnTableId, ERROR_NUMBER (), ERROR_MESSAGE (), ERROR_LINE (), ERROR_SEVERITY (), ERROR_STATE (), ERROR_PROCEDURE (), GETDATE (), SYSTEM_USER)
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

-- Compara la Table y Schema que recibe contra los de la otra DB.
CREATE PROCEDURE SPCompareTables @Db1 VARCHAR (MAX), 
									@Db2 VARCHAR (MAX), 
									@AnId NUMERIC (18, 0), 
									@Db1Schema VARCHAR (MAX), 
									@Db1Table VARCHAR (MAX) AS
	BEGIN
		BEGIN TRY
			/*
			-- Se deja comentado el manejo de transacciones en caso de ser necesario.
			BEGIN TRANSACTION
			*/
			SET NOCOUNT ON
			DECLARE @Statement NVARCHAR (MAX),
				@Cantidad INT,
				@TableExists INT,
				@SchemaExists INT

			-- Busca la Table y el Schema en la segunda DB.
			SET @Statement = 'SELECT @SchemaExists = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLES
								WHERE TABLE_SCHEMA = ''' + @Db1Schema + ''''
			EXECUTE SP_EXECUTESQL @Statement, N'@SchemaExists INT OUTPUT', @SchemaExists = @Cantidad OUTPUT
			IF (@Cantidad = 0)
				BEGIN
				INSERT INTO AnalisisTablas (AnalisisDbsId, SchemaDb2, TableDb2, SchemaDb2Exists, TableDb2Exists)
					VALUES (@AnId, @Db1Schema, @Db1Table, 'No', 'No se comprueba')
				RAISERROR ('ERROR! No existe el Esquema en la segunda Base de Datos.',15 ,1)
				END
			SET @Statement = 'SELECT @TableExists = COUNT (*)
								FROM ' + @Db2 +'.INFORMATION_SCHEMA.TABLES
								WHERE TABLE_NAME = ''' + @Db1Table + ''''
			EXECUTE SP_EXECUTESQL @Statement, N'@TableExists INT OUTPUT', @TableExists = @Cantidad OUTPUT
			IF (@Cantidad = 0)
				BEGIN
				INSERT INTO AnalisisTablas (AnalisisDbsId, SchemaDb2, TableDb2, SchemaDb2Exists, TableDb2Exists)
					VALUES (@AnId, @Db1Schema, @Db1Table, 'Si', 'No')
				RAISERROR ('ERROR! No existe la Tabla en la segunda Base de Datos.',15 ,1) 
			END

			-- Control de PKs.
			DECLARE @CantPkDb1 INT,
				@CantPkDb2 INT,
				@Pk INT,
				@PkDb1 VARCHAR (MAX),
				@PkDb2 VARCHAR (MAX)
			SET @Statement = 'SELECT @Pk = COUNT (*)
								FROM ' + @Db1 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''PRIMARY KEY''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Pk INT OUTPUT', @Pk = @CantPkDb1 OUTPUT
			SET @Statement = 'SELECT @Pk = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db2 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''PRIMARY KEY''
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
			DECLARE @CantFkDb1 INT,
				@CantFkDb2 INT,
				@Fk INT,
				@FkDb1 VARCHAR (MAX),
				@FkDb2 VARCHAR (MAX)
			SET @Statement = 'SELECT @Fk = COUNT (*)
								FROM ' + @Db1 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''FOREIGN KEY''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Fk INT OUTPUT', @Fk = @CantFkDb1 OUTPUT
			SET @Statement = 'SELECT @Fk = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db2 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''FOREIGN KEY''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Fk INT OUTPUT', @Fk = @CantFkDb2 OUTPUT		
			SET @FkDb1 = CONVERT(NVARCHAR,@CantFkDb1) + ' Claves Foraneas'
			SET @FkDb2 = CONVERT(NVARCHAR,@CantFkDb2) + ' Claves Foraneas'

			-- Control de CHECK.
			DECLARE @CantCheckDb1 INT,
				@CantCheckDb2 INT,
				@Check INT,
				@CheckDb1 VARCHAR (MAX),
				@CheckDb2 VARCHAR (MAX)
			SET @Statement = 'SELECT @Check = COUNT (*)
								FROM ' + @Db1 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''Check''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Check INT OUTPUT', @Check = @CantCheckDb1 OUTPUT
			SET @Statement = 'SELECT @Check = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db2 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''Check''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Check INT OUTPUT', @Check = @CantCheckDb2 OUTPUT		
			SET @CheckDb1 = CONVERT(NVARCHAR,@CantCheckDb1) + ' Check'
			SET @CheckDb2 = CONVERT(NVARCHAR,@CantCheckDb2) + ' Check'
			
			-- Control de UNIQUE.
			DECLARE @CantUniqueDb1 INT,
				@CantUniqueDb2 INT,
				@Unique INT,
				@UniqueDb1 VARCHAR (MAX),
				@UniqueDb2 VARCHAR (MAX)
			SET @Statement = 'SELECT @Unique = COUNT (*)
								FROM ' + @Db1 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''UNIQUE''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Unique INT OUTPUT', @Unique = @CantUniqueDb1 OUTPUT
			SET @Statement = 'SELECT @Unique = COUNT (*)
								FROM ' + @Db2 + '.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS T
									JOIN ' + @Db2 + '.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS C ON T.CONSTRAINT_NAME = C.CONSTRAINT_NAME
										AND T.TABLE_NAME = C.TABLE_NAME
								WHERE C.TABLE_NAME = ''' + @Db1Table + '''
									AND C.TABLE_SCHEMA = ''' + @Db1Schema + '''
									AND T.CONSTRAINT_TYPE = ''UNIQUE''
								GROUP BY C.TABLE_NAME'
			EXECUTE SP_EXECUTESQL @Statement, N'@Unique INT OUTPUT', @Unique = @CantUniqueDb2 OUTPUT		
			SET @UniqueDb1 = CONVERT(NVARCHAR,@CantUniqueDb1) + ' Unique'
			SET @UniqueDb2 = CONVERT(NVARCHAR,@CantUniqueDb2) + ' Unique'

			-- Persisto la auditoria para el analisis de tablas.
			INSERT INTO AnalisisTablas (AnalisisDbsId ,SchemaDb2 ,SchemaDb2Exists ,TableDb2 ,TableDb2Exists ,PkDb1 ,PkDb2 ,FkDb1 ,FkDb2 ,UniqueDb1 ,UniqueDb2 ,CheckDb1 ,CheckDb2) 
				VALUES(@AnId,@Db1Schema ,'Si' ,@Db1Table ,'Si' ,@PkDb1 ,@PkDb2 ,@FkDb1 ,@FkDb2 ,@UniqueDb1 ,@UniqueDb2 ,@CheckDb1 ,@CheckDb2);
			
			-- Comienza auditoria para el analisis de columnas.
			DECLARE @AnTableId INT,
				@IsIdentity INT,
				@ColumnName VARCHAR(MAX),
				@Position INT,
				@ColumnDefault VARCHAR(MAX),
				@DataType VARCHAR(MAX),
				@MaxLength INT
			SET @AnTableId = @@IDENTITY
			SET @Statement = 'DECLARE CompareColumnsCursor CURSOR FOR
								SELECT DISTINCT C.is_identity AS IsIdentity, I.COLUMN_NAME AS ColumnName, I.ORDINAL_POSITION AS Position, I.COLUMN_DEFAULT AS ColumnDefault, I.DATA_TYPE AS DataType, C.max_length AS MaxLength
								FROM ' + @Db1 + '.SYS.COLUMNS AS C
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
								WHERE I.TABLE_NAME = ''' + @Db1Table + '''
									AND I.TABLE_SCHEMA = ''' + @Db1Schema + ''''
			EXECUTE SP_EXECUTESQL @Statement
			OPEN CompareColumnsCursor
			FETCH NEXT FROM CompareColumnsCursor
				INTO @IsIdentity, @ColumnName, @Position, @ColumnDefault, @DataType, @MaxLength
			WHILE (@@FETCH_STATUS = 0)
				BEGIN
					EXECUTE SPCompareColumns @Db1, @Db2, @AnTableId, @Db1Schema, @Db1Table, @IsIdentity, @ColumnName, @Position, @ColumnDefault, @DataType, @MaxLength
					FETCH NEXT FROM CompareColumnsCursor
						INTO @IsIdentity, @ColumnName, @Position, @ColumnDefault, @DataType, @MaxLength
				END
			CLOSE CompareColumnsCursor
			DEALLOCATE CompareColumnsCursor

			/*
			-- Se realiza un Commit cuando llega sin errores.
			COMMIT TRANSACTION
			*/
		END TRY
		BEGIN CATCH
			PRINT 'Se ha producido un error, revisar el log de errores.'
			INSERT INTO ErrorLog (AnalisisId, ErrorNumber, ErrorMessage, ErrorLine, ErrorSeverity, ErrorState, ErrorProcedure, FechaHora, Usuario)
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
CREATE PROCEDURE SPCompareDbs @Db1 VARCHAR (MAX), 
								@Db2 VARCHAR (MAX) AS
	BEGIN
		BEGIN TRY
			/*
			-- Se deja comentado el manejo de transacciones en caso de ser necesario.
			BEGIN TRANSACTION
			*/
			SET NOCOUNT ON

			-- Verifica la existencia de ambas DBs.
			IF (DB_ID (@Db1) IS NOT NULL)
				BEGIN
					IF (DB_ID (@Db2) IS NOT NULL)
						BEGIN
							INSERT INTO AnalisisDbs (Db1, Db2, Db1Exists, Db2Exists)
								VALUES (@Db1, @Db2, 'Si', 'Si')
							DECLARE @AnId NUMERIC (18, 0)
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

			-- Comienza auditoria para el analisis de tablas.
			DECLARE @Statement NVARCHAR (MAX),
				@Db1Schema VARCHAR (MAX),
				@Db1Table VARCHAR (MAX)
			SET @Statement = 'DECLARE CompareTablesCursor CURSOR FOR
								SELECT S.name AS SchemaName, T.name AS TableName
								FROM ' + @Db1 + '.SYS.SCHEMAS AS S
									JOIN ' + @Db1 + '.SYS.TABLES AS T ON S.schema_id = T.schema_id'
			EXECUTE SP_EXECUTESQL @Statement
			OPEN CompareTablesCursor
			FETCH NEXT FROM CompareTablesCursor
			INTO @Db1Schema, @Db1Table
			WHILE (@@FETCH_STATUS = 0)
				BEGIN
				EXECUTE SPCompareTables @Db1, @Db2, @AnId, @Db1Schema, @Db1Table
				FETCH NEXT FROM CompareTablesCursor
					INTO @Db1Schema, @Db1Table
				END
			CLOSE CompareTablesCursor
			DEALLOCATE CompareTablesCursor
			/*
			-- Se realiza un Commit cuando llega sin errores.
			COMMIT TRANSACTION
			*/
		END TRY
		BEGIN CATCH
			PRINT 'Se ha producido un error, revisar el log de errores.'
			INSERT INTO ErrorLog (AnalisisId, ErrorNumber, ErrorMessage, ErrorLine, ErrorSeverity, ErrorState, ErrorProcedure, FechaHora, Usuario)
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


/*
	--Prueba de generador de scritp

	--Procedure para crear DB.
	CREATE PROCEDURE SPCreateDB @AnId NUMERIC (18, 0), 
									@Db1 VARCHAR (MAX) AS
		BEGIN
			DECLARE @Statement NVARCHAR (MAX)
			SET @Statement = 'CREATE DATABASE ' + @Db1 + '-v2.0 GO USE ' + @Db1 + '-v2.0 GO'
			INSERT INTO AlterScript (AnalisisDbsID, Script)
				VALUES (@AnId, @Statement)
		END
	GO

	--////procedura a llamar cuando se crea DB para iterar tablas

	--////procedura a llamar cuando se crea DB para iterar columns

	--Procedure para crear Schema.
	CREATE PROCEDURE SPCreateSchema @AnId NUMERIC (18, 0), 
										@Db1 VARCHAR (MAX), 
										@Db1Schema VARCHAR (MAX) AS
		BEGIN
			DECLARE @Statement NVARCHAR (MAX)
			SET @Statement = 'CREATE SCHEMA ' + @Db1Schema + ' GO'
			INSERT INTO AlterScript (AnalisisDbsID, Script)
				VALUES (@AnId, @Statement)
		END
	GO

	--Procedure para crear Table.
	CREATE PROCEDURE SPCreateTable @AnId NUMERIC (18, 0), 
									@Db1 VARCHAR (MAX), 
									@Db1Table VARCHAR (MAX), 
									@Db1Schema VARCHAR (MAX) AS
		BEGIN
			DECLARE @Statement NVARCHAR (MAX),
				@Script NVARCHAR (MAX)
			SET @Script = 'CREATE TABLE ' + @Db1Schema + ' ('
			SET @Statement = 'DECLARE CompareColumnsCursor CURSOR FOR
								SELECT DISTINCT C.is_identity AS IsIdentity, i.COLUMN_NAME AS ColumnName, I.ORDINAL_POSITION AS Position, I.COLUMN_DEFAULT AS ColumnDefault, I.DATA_TYPE AS DataType, C.max_length AS MaxLength
								FROM ' + @Db1 + '.SYS.COLUMNS AS C
									JOIN ' + @Db1 + '.INFORMATION_SCHEMA.COLUMNS AS I ON C.name = I.COLUMN_NAME
								WHERE I.TABLE_NAME = ''' + @Db1Table + '''
									AND I.TABLE_SCHEMA = ''' + @Db1Schema + ''''
			EXECUTE SP_EXECUTESQL @Statement
			OPEN CompareColumnsCursor
			FETCH NEXT FROM CompareColumnsCursor
				INTO @IsIdentity, @ColumnName, @Position, @ColumnDefault, @DataType, @MaxLength
			WHILE (@@FETCH_STATUS = 0)
				BEGIN
					SET @Script = @Script + @ColumnName + ' ' + @DataType + ' (' + @MaxLength +') ' + @ColumnDefault + ''
					IF (@IsIdentity = 1)
						BEGIN
							SET @Script = @Script + ' IDENTITY'
						END
					IF (@@FETCH_STATUS = 0)
						BEGIN
							SET @Script = @Script + ', '
						END     
					FETCH NEXT FROM CompareColumnsCursor
						INTO @IsIdentity, @ColumnName, @Position, @ColumnDefault, @DataType, @MaxLength
				END
			CLOSE CompareColumnsCursor
			DEALLOCATE CompareColumnsCursor
			SET @Script = @Script + ') GO'
			INSERT INTO AlterScript (AnalisisDbsID, Script)
				VALUES (@AnId, @Statement)
		END
	GO

*/

