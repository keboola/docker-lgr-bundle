#' Connect to Amazon Redshift database
#' 
#' @param jdbcUrl JDBC connection string
#' @param driver Path to the JDBC driver 
#' @param username Database user name
#' @param password Database password
#' @param schema Database schema
#' @return void
redshift.connect <- function(jdbcUrl, driver, username, password, schema) {
	driver <- JDBC("org.postgresql.Driver", driver, identifier.quote="`")
	# if url has GET parameters already, then concat name and passwrod after &
	lead <- ifelse(grepl("\\?", jdbcUrl), "&", "?")
	url <- paste0(jdbcUrl, lead, "user=", username, "&password=", password)
	conn <- dbConnect(driver, url)
	# make conn global variable under the name 'redshift.conn' and 'redshift.schema'
	assign('redshift.conn', conn, envir = .GlobalEnv)
	assign('redshift.schema', schema, envir = .GlobalEnv)
	# set schema search path so that we don't have to use fully qualified names
	redshift.update(paste0("SET search_path TO '", schema, "'"))
}

#' Prepare a SQL query with quoted parameters
#' 
#' @param sql SQL string, parameter placeholders are marked with ?
#' @param ... Query parameters, number of parameters must be same as number of 
#'  question marks
#' @return SQL string
redshift.prepareStatement <- function(sql, ...) {
	parameters <- list(...)
	quotedParametrs <- lapply(
		X = parameters, 
		function (value) {
			# escape the quotes (if any) in a value
			value <- gsub("'", "''", value)
			# quote the value
			value <- paste0("'", value, "'")
		}
	)
	if (length(quotedParametrs) > 0) {
		for (i in 1:length(quotedParametrs)) {
			sql <- sub("\\?", quotedParametrs[[i]], sql)
		}
	}
	sql
}

#' Select data from database
#' 
#' @param sql Query string, may contain placeholders ? for parameters
#' @param ... Query parameters 
#' @return A dataframe with results
redshift.select <- function(sql, ...) {
	sql <- redshift.prepareStatement(sql, ...)
	tryCatch(
		{
			ret <- dbGetQuery(redshift.conn, sql)
		},
		error = function(e) {
			stop(paste0("Failed to execute query ", e, " q: (", sql, ") "))
		}
	)
	ret
}


#' Update/Insert data to database
#' 
#' @param sql Query string, may contain placeholders ? for parameters
#' @param ... Query parameters 
#' @return void
redshift.update <- function(sql, ...) {
	sql <- redshift.prepareStatement(sql, ...)
	tryCatch(
		{
			ret <- dbSendUpdate(redshift.conn, sql)
		},
		error = function(e) {
		    stop(paste0("Failed to execute query ", e, " q: (", sql, ") "))
		}
	)
	ret
}


#' Save a dataframe to database using bulk inserts. The table will be created to accomodate to data frame columns.
#'
#' @param df A data frame, column names of data frame must correspond to column names of table
#' @param table Name of the table.
#' @param rowNumbers If true then the table will contain a column named 'row_num' with sequential row index
#' @param incremental If true then the table will not be recreated, only data will be inserted
#' @param forcedColumnTypes List of column names and their respective types in database.
#' @return void
redshift.saveDataFrame <- function(dfRaw, table, rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes) {
  # drop the table if already exists and loading is not incremental
  tableFull <- paste0(redshift.schema, '.', table)
  if (!incremental) {
    # convert factors to strings
    df <- data.frame(lapply(colnames(dfRaw), function(colname)
    {
      if (is.factor(dfRaw[[colname]])) {
        as.character(dfRaw[[colname]])
      } else {
        dfRaw[[colname]]
      }
    }
    ), stringsAsFactors = FALSE)
        colnames(df) <- colnames(dfRaw)
        
		# get column types
		types <- lapply(df, typeof)
    
		# convert column types to database types and create list of column defininitions
		if (rowNumbers) {
			columns <- list("row_num INTEGER")
		} else {
			columns <- list()		
		}
	 	for (name in names(types)) {
	 		type <- "";
	 		if (types[[name]] == 'double') {
	 			type <- 'DECIMAL (30,20)'
	 		}
	 		if (types[[name]] == 'integer') {
	 			type <- 'BIGINT'
	 		}
	 		if (types[[name]] == 'logical') {
	 			type <- 'INTEGER'
	 		}
	 		if (types[[name]] == 'character') {
	 			type <- 'VARCHAR(2000)'
	 		}
	 		if (types[[name]] == 'NULL') {
	 			type <- 'INTEGER'
	 		}

            if (!missing(forcedColumnTypes) && (name %in% names(forcedColumnTypes))) {
                type <- as.character(forcedColumnTypes[[name]])
            }
	 		if (type == "") {
	 			stop(paste0("Unhandled column type ", types[[name]]))
	 		}
	 		columns <- c(columns, paste0('"', name, '" ', type))
	 	}
        # drop the table if necessary
		if (redshift.tableExists(table)) {
		  redshift.update(paste0("DROP TABLE ", tableFull, ";"))
		}
		# create the table
	 	sql <- paste0("CREATE TABLE ", tableFull, " (", paste(columns, collapse = ", "), ");")
        redshift.update(sql)
	} else {
       df <- dfRaw
	}

	# Maximum size of a statement is 16MB http://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql.html	
	# set the limit a little bit lower, because the counting is not precise
	sqlLimit <- 5000000 
	# create query header
	colNames <- colnames(df)
	colNames <- lapply(
		X = colNames,
		function (value) {
			value <- paste0('"', value, '"')
		}
	)
	if (rowNumbers) {
		sqlHeader <- paste0("INSERT INTO ", tableFull, " (row_num, ", paste(colNames, collapse = ", "), ") VALUES ")
	} else {
		sqlHeader <- paste0("INSERT INTO ", tableFull, " (", paste(colNames, collapse = ", "), ") VALUES ")			
	}
  
	# list for row values
	sqlVals <- list()
	sqlLength <- 0

	if (nrow(df) > 0) {
		# data frame is non-empty 
		for (i in 1:nrow(df)) {
			# save row so as not to modify the original data frame
			row <- df[i,]
			# escape and quote all values
			row <- lapply(
				X = row, 
				function (value) {
					if (is.null(value) || is.na(value)) {
						value <- "NULL"
					} else {
						# escape the quotes (if any) in a value
						value <- gsub("'", "''", value)
						# quote the value
						value <- paste0("'", value, "'")
					}
				}
			)
			# produce a single row of values
			if (rowNumbers) {
				sqlVal <- paste0("('", i, "', ", paste(row, collapse = ", "), ")")
			} else {
				sqlVal <- paste0("(", paste(row, collapse = ", "), ")")
			}
			# store the row in list
			sqlVals <- c(sqlVals, sqlVal)
			# keep track of length of the list
			sqlLength <- sqlLength + nchar(sqlVal)
			if (sqlLength > sqlLimit) {
				# query length is over limit, execute it
				sql <- paste0(sqlHeader, paste(sqlVals, collapse = ", "))
				redshift.update(sql)
				# clear row values
				sqlLength <- 0
				sqlVals <- list()
			}
		}
	}
	# if there are some rows left, insert them 
	if (sqlLength > 0) {
		sql <- paste0(sqlHeader, paste(sqlVals, collapse = ", "))
		redshift.update(sql)
	}
	TRUE
}


#' Verify that a table exists in database
#'
#' @param tableName Name of the table (without schema).
#' @return TRUE if the table exists, FALSE otherwise.
redshift.tableExists <- function(tableName) {
	res <- redshift.select("SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema ILIKE ? AND table_name ILIKE ?;", redshift.schema, tableName);
	ret <- res[1, 'count'] > 0
	ret
}


#' Get list of columns in table and their datatypes
#' 
#' @param tableName Name of the table (including schema).
#' @return Dataframe with columns 'column' and 'dataType'
redshift.columnTypes <- function(tableName) {
    ret <- redshift.select("SELECT column_name, data_type FROM information_schema.columns WHERE (table_schema || '.' || table_name) ILIKE ?;", tableName);
    colnames(ret) <- c('column', 'dataType')    
    retVector <- as.vector(ret[,'dataType'])
    names(retVector) <- ret[,'column']
    retVector
}