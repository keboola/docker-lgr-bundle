#' This script provides a simple initialization wrapper for command line arguments and
#'  Amazon Redshift database connection.
#'
#' Usage from R script:
#' Declare a method: module.run <- function() {
#'
#' Now the following variables are initialized:
#' wrapper.sourceTable - table to read data from
#' wrapper.params - arbitrary parameters
#'
#' Command line arguments:
#' moduleToExecute
#' dbDriver
#' dbConnectionString
#' dbUser
#' dbPassword
#' dbSchema
#' sourceTable
#' params
#' debug

#' Logging wrapper which prints Java like stack trace in case of error.
#' Comes from http://stackoverflow.com/a/24884348/41640
#'
wrapper.tryCatch = function(expr, silentSuccess = FALSE, stopIsFatal = TRUE) {
  hasFailed = FALSE
  messages = list()
  warnings = list()
  logger = function(obj) {
    # Change behaviour based on type of message
    level = sapply(class(obj), switch, debug="DEBUG", message="INFO", warning="WARN", caughtError = "ERROR",
                   error=if (stopIsFatal) "FATAL" else "ERROR", "")
    level = c(level[level != ""], "ERROR")[1]
    simpleMessage = switch(level, DEBUG=,INFO=TRUE, FALSE)
    quashable = switch(level, DEBUG=,INFO=,WARN=TRUE, FALSE)

    # Format message
    time  = format(Sys.time(), "%Y-%m-%d %H:%M:%OS3")
    txt   = conditionMessage(obj)
    if (!simpleMessage) txt = paste(txt, "\n", sep="")
    msg = paste(time, level, txt, sep=" ")
    calls = sys.calls()
    calls = calls[1:length(calls)-1]
    trace = limitedLabels(c(calls, attr(obj, "calls")))
    if (!simpleMessage && length(trace) > 0) {
      trace = trace[length(trace):1]
      msg = paste(msg, "  ", paste("at", trace, collapse="\n  "), "\n", sep="")
    }

    # Output message
    if (silentSuccess && !hasFailed && quashable) {
      messages <<- append(messages, msg)
      if (level == "WARN") warnings <<- append(warnings, msg)
    } else {
      if (silentSuccess && !hasFailed) {
        cat(paste(messages, collapse=""))
        hasFailed <<- TRUE
      }
      cat(msg)
    }

    # Muffle any redundant output of the same message
    optionalRestart = function(r) { res = findRestart(r); if (!is.null(res)) invokeRestart(res) }
    optionalRestart("muffleMessage")
    optionalRestart("muffleWarning")
  }
  vexpr = withCallingHandlers(withVisible(expr),
                              debug=logger, message=logger, warning=logger, caughtError=logger, error=logger)
  if (silentSuccess && !hasFailed) {
    cat(paste(warnings, collapse=""))
  }
  if (vexpr$visible) vexpr$value else invisible(vexpr$value)
}

#' Get full path of the current script
#' @return string full path, without trailing delimiter.
wrapper.scriptPath <- function() {
  if (!exists("wrapper.path")) {
  	args <- commandArgs(trailingOnly = FALSE)
  	scriptPath <- normalizePath(dirname(sub("^--file=", "", args[grep("^--file=", args)])))
    #stop(class(scriptPath))
    if (length(scriptPath) == 0) {
      # if that failed, try another method
      scriptPath <- dirname(sys.frame(1)$ofile);
    }
  	assign('wrapper.path', scriptPath, envir = .GlobalEnv)
  }
  wrapper.path
}


#' Initialize the wrapper. Connects to database, processe command line arguments and
#'  and makes global variables available.
#'
wrapper.run <- function(moduleToExecute = NULL, jdbcDriver = NULL, jdbcUrl = NULL,
    dbUser = NULL, dbPassword = NULL, dbSchema = NULL, workingDir = NULL,
    sourceTable = NULL, params = NULL, debug = NULL) {
	args <- commandArgs(trailingOnly = TRUE)

    if (is.null(moduleToExecute)) {
      	moduleToExecute <- args[1]
    }
    if (is.null(jdbcDriver)) {
      	jdbcDriver <- args[2]
    }
    if (is.null(jdbcUrl)) {
      	jdbcUrl <- args[3]
    }
    if (is.null(dbUser)) {
      	dbUser <- args[4]
    }
    if (is.null(dbPassword)) {
      	dbPassword <- args[5]
    }
    if (is.null(dbSchema)) {
      	dbSchema <- args[6]
    }
    if (is.null(workingDir)) {
      	workingDir <- args[7]
    }
    if (is.null(sourceTable)) {
      	sourceTable <- args[8]
    }
    if (is.null(params)) {
      	params <- args[9]
    }

	if (is.null(debug) || is.na(debug)) {
        debug <- args[10]
        debug <- as.integer(debug)
        if (is.null(debug) || is.na(debug)) {
            debug <- 0
        }
    }

    # make working dir globally available
	assign('wrapper.workingDir', workingDir, envir = .GlobalEnv)
    assign('wrapper.debug', debug, envir = .GlobalEnv)
    # make working dir also library dir so that parallel runs do not clash with each other
	.libPaths(c(.libPaths(), wrapper.workingDir))

    if (!interactive()) {
        if (debug) {
            # push everything to log file
            debugFile <- file(file.path(wrapper.workingDir, "debug.log"), open="wt")
            sink(file = debugFile, append = TRUE, type = "message")
            sink(file = debugFile, append = TRUE, type = "output")
            print(paste0("Debug started: ", Sys.time()));
        } else {
            # push output to nul, and leave messages intact
            if (Sys.info()['sysname'] == 'Windows') {
              debugFile = file("NUL")
            } else {
              debugFile = file("/dev/null")
            }
            sink(file = debugFile, append = TRUE, type = "output")
        }
    }

    # Install required packages
	wrapper.install()

	# include Redshift functions which rely on the loaded packages
	source(file.path(wrapper.scriptPath(), "redshift.R"))

	keyValTable <- 'r__key_val_results'
	fileNamesTable <- 'r__file_names'
	tableNamesTable <- 'r__table_names'

	# some very basic input checking
	if (is.na(moduleToExecute) || is.null(moduleToExecute)) {
		stop("I am missing name of the module to execute in command line arguments.")
	}

	if (is.na(jdbcUrl) || is.null(jdbcUrl) || is.na(jdbcDriver) || is.null(jdbcDriver) || is.na(dbUser) || is.null(dbUser) || is.na(dbPassword) || is.null(dbPassword)) {
		stop("I am missing database connection settings in command line arguments.")
	}

	if (is.na(sourceTable) || is.null(sourceTable)) {
		stop("I am missing source table in command line arguments.")
	}

	if (is.null(workingDir)) {
		stop("I am missing working directory in command line arguments.")
	}

	# try to estabilish databse connection
	tryCatch(
		{
			redshift.connect(jdbcUrl, jdbcDriver, dbUser, dbPassword, dbSchema)
		},
		error = function(e) {
		  wrapper.flushSinks()
		  stop(paste("Failed to connect to database ", e, sep = ""))
		}
	)
	if (redshift.tableExists(keyValTable)) {
		redshift.update(paste("DROP TABLE ", keyValTable, ";", sep = ""))
	}
	if (redshift.tableExists(fileNamesTable)) {
		redshift.update(paste("DROP TABLE ", fileNamesTable, ";", sep = ""))
	}
	if (redshift.tableExists(tableNamesTable)) {
		redshift.update(paste("DROP TABLE ", tableNamesTable, ";", sep = ""))
	}
	redshift.update(paste("CREATE TABLE ", keyValTable, " (name VARCHAR(200), value VARCHAR(200), grouping VARCHAR(200), PRIMARY KEY (name));"), sep = "")
	redshift.update(paste("CREATE TABLE ", fileNamesTable, " (name VARCHAR(200), value VARCHAR(200), id INTEGER, PRIMARY KEY (name));"), sep = "")
	redshift.update(paste("CREATE TABLE ", tableNamesTable, " (name VARCHAR(200));"), sep = "")

	# load the module
	source(moduleToExecute)

    # get parameters required by the module
	paramsJ <- fromJSON(params, flatten = TRUE)

	requiredParams <- module.parameters()

	# install module packages
	wrapper.install(requiredParams$packages)
	requiredParams[['packages']] <- NULL

	# verify parameters required by the module
    errors <- vector()
	for (key in names(requiredParams)) {
    name <- unlist(strsplit(key, ".", fixed = TRUE))
  	tryCatch({
  	  tmpData <- wrapper.checkParam(paramsJ, name, requiredParams[[key]], key)
      paramsJ <- tmpData[["data"]]
      errors <- c(errors, tmpData[["warnings"]])
  	}, error = function(e) {
  	  errors <- c(errors, e$message)
  	})
	}
    errors <- errors[!is.na(errors)]
	if (length(errors) > 0) {
	  wrapper.flushSinks()
	  stop(paste0("Some required parameters for module ", moduleToExecute, " were not provided: ", paste(errors, collapse = " ")))
	}

	# make variables available to module
	assign('wrapper.sourceTable', paste0(redshift.schema, ".", sourceTable), envir = .GlobalEnv)
	assign('wrapper.params', paramsJ, envir = .GlobalEnv)

	# create an empty data frame for the key-value storage
	keyValue <- data.frame(name = character(), value = character(), grouping = integer(), stringsAsFactors = FALSE)
	# and make it globally available (it should be accessed via wrapper.saveValue())
	assign('wrapper.keyValue', keyValue, envir = .GlobalEnv)

	# create an empty data frame for the file storage
	fileNames <- data.frame(name = character(), value = character(), grouping = integer(), stringsAsFactors = FALSE)
	# and make it globally available (it should be accessed via wrapper.saveFileName())
	assign('wrapper.fileNames', fileNames, envir = .GlobalEnv)

	# make globally available the list of tables (it should be accessed via wrapper.saveDataFrame())
	tableNames <- data.frame(name = character(), stringsAsFactors = FALSE)
    assign('wrapper.tableNames', tableNames, envir = .GlobalEnv)

	# run the module
	if (interactive()) {
	  module.run()
	} else {
  	tryCatch(
      wrapper.tryCatch(
        {
      	  ret <- module.run()
      	  if (ret != TRUE) {
      	    stop(paste0("Module failed to return TRUE, return value: ", ret))
      	  }
      	},
        stopIsFatal = FALSE),
      error = function(e) {
        stop(paste0("Module failed, error: ", e$message))
      }, finally = {
        if (!interactive()) {
          wrapper.flushSinks()
        }
      }
    )
	}

	# flush key value store to database
    redshift.saveDataFrame(wrapper.keyValue, keyValTable, rowNumbers = FALSE, incremental = TRUE)
    redshift.saveDataFrame(wrapper.fileNames, fileNamesTable, rowNumbers = FALSE, incremental = TRUE)
    redshift.saveDataFrame(wrapper.tableNames, tableNamesTable, rowNumber = FALSE, incremental = TRUE)

    TRUE
}


#' Recursive function to check a parameter provided in JSON data. Note that this function is called for
#'  each parameter (name) separately and it tries to find that 'name' in the provided 'data'. It will
#'  return the data possibly with coerced values
#'
#' @param data JSON parsed data
#' @param name Name of the parameter, either a string or a vector of characters if the parameter is nested.
#' @param dataType String name of the R data type required
#' @param fullName Arbitrary named used for global identification of the parameter.
#' @return Partially modified data.
wrapper.checkParam <- function(data, name, dataType, fullName) {
  warnings <- NA
  if (length(name) > 1) {
    # name is still nested, we need to go deeper
    tmpData <- wrapper.checkParam(data[[name[1]]], name[-1], dataType, fullName)
    data[[name[1]]] <- tmpData[["data"]]
    warnings <- tmpData[["warnings"]]
  } else if (length(name) == 1) {
    # this is just for reporting errors
    if (name == fullName) {
      fullName <- 'root'
    }
    # name is present in data (i.e. parameter value is supplied)
    if (name[1] %in% names(data)) {
      if (typeof(data[[name[1]]]) != dataType) {
        # parametr value differs from the required one
        if (dataType == "integer") {
          # try to cast
          tryCatch({
            data[[name[1]]] <- as.integer(data[[name[1]]])
          }, warning = function(w) {
            warnings <<- paste0("Parameter ", name, " in ", fullName, " has different datatype, got ",
                        typeof(data[[name[1]]]), " expected ", dataType, ", and cannot be converted: ", w$message)
          })
        } else if (dataType == "double") {
          # try to cast
          tryCatch({
            data[[name[1]]] <- as.double(data[[name[1]]])
          }, warning = function(w) {
            warnings <<- paste0("Parameter ", name, " in ", fullName, " has different datatype, got ",
                        typeof(data[[name[1]]]), " expected ", dataType, ", and cannot be converted: ", w$message)
          })
        } else if (dataType == "logical") {
          # try to cast
          tryCatch({
            # convert 1,0 and TRUE,FALSE and "TRUE","FALSE"
            data[[name[1]]] <- as.logical(data[[name[1]]])
            if (is.na(data[[name[1]]])) {
              # convert "1","0"
              data[[name[1]]] <- as.logical(as.integer(data[[name[1]]]))
            }
          }, warning = function(w) {
            warnings <<- paste0("Parameter ", name, " in ", fullName, " has different datatype, got ",
                                typeof(data[[name[1]]]), " expected ", dataType, ", and cannot be converted: ", w$message)
          })
        } else if (dataType == "lg_column") {
          # 'lg_column' is our own special type used to lowercase a string
          data[[name[1]]] <- tolower(data[[name[1]]])
        } else {
          warnings <- paste0("Parameter ", name, " in ", fullName, " has different datatype, got ",
                      typeof(data[[name[1]]]), " expected ", dataType, ".")
        }
      }
    } else {
      warnings <- paste0("Parameter ", name, " is missing in ", fullName, ".")
    }
  }
  list("data" = data, "warnings" = warnings)
}


#' Silence all but error output from a command.
#'
#' Note: this function does nothing if the the gloal wrapper.debug variable
#'  is set to FALSE.
#'  @return Command return value.
wrapper.silence <- function(command) {
	if (!wrapper.debug) {
		msg.trap <- capture.output(suppressPackageStartupMessages(suppressMessages(suppressWarnings(ret <- command))))
	} else {
		ret <- command
	}
	ret
}


#' Install and load all required libraries.
#'
wrapper.install <- function(packages = c("rJava", "RJDBC", "jsonlite")) {
  if (!is.null(packages) && (length(packages) > 0)) {
    # repository <- "http://cran.us.r-project.org"
    # use the czech mirror to increase speed slightly
    repository <- "http://mirrors.nic.cz/R/"
    # get only packages not yet installed
    packagesToInstall <- packages[which(!(packages %in% rownames(installed.packages())))]
    if (length(packagesToInstall) > 0) {
      wrapper.silence(
        install.packages(
          pkgs = packagesToInstall,
          lib = wrapper.workingDir,
          repos = repository,
          quiet = TRUE,
          verbose = FALSE,
          dependencies = c("Depends", "Imports", "LinkingTo"),
          INSTALL_opts = c("--no-html")
          )
        )
    }
    # load all packages
    lapply(packages, function (package) {
      wrapper.silence(library(package, character.only = TRUE, quietly = TRUE))
    })
  }
}

#' Flush all sinks
#'
wrapper.flushSinks <- function() {
  for(i in seq_len(sink.number())){
    sink(NULL)
  }
}

#' Split a string into tokens using the given split character.
#'
#' @param string Arbitrary string.
#' @param splitChar Split character.
#' @param asLogical If true than a vector of TRUEs indexed by token name will be returned
#'  if false (default) then a vector of tokens will be indexed.
wrapper.split <- function(string, splitChar, asLogical = FALSE)
{
  # split
  tokens <- strsplit(string, splitChar)[[1]]
  # trim whitespace from each item
  tokens <- lapply(tokens, function (x) {gsub("^\\s+|\\s+$", "", x)})
  if (asLogical) {
    logtokens <- logical()
    lapply(tokens, function (x) {logtokens[x] <<- TRUE})
    tokens <- logtokens
  }
  tokens
}

#' Save an arbitrary simple value.
#'
#' @param key String key name.
#' @param value Arbitrary value.
#' @param grouping Optional grouping of values if they are related
wrapper.saveValue <- function(name, value, grouping = 0) {
	newRow <- data.frame(name, value, grouping, stringsAsFactors = FALSE)
	kv <- rbind(wrapper.keyValue, newRow)
	assign('wrapper.keyValue', kv, envir = .GlobalEnv)
}


#' Save reference to a file.
#'
#' @param key String file name.
#' @param value String physical file path.
wrapper.saveFileName <- function(name, value) {
	newRow <- data.frame(name, value, stringsAsFactors = FALSE)
	kv <- rbind(wrapper.fileNames, newRow)
	assign('wrapper.fileNames', kv, envir = .GlobalEnv)
}


#' Save a dataframe to database using bulk inserts. The table will be created to accomodate to data frame columns.
#'
#' @param dataFrame A data frame, column names of data frame must correspond to column names of table
#' @param tableName Name of the table.
#' @param rowNumbers If true then the table will contain a column named 'row_num' with sequential row index
#' @param incremental If true then the table will not be recreated, only data will be inserted
#' @return void
wrapper.saveDataFrame <- function(dataFrame, tableName, rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes) {
    if (missing(forcedColumnTypes)) {
        redshift.saveDataFrame(dataFrame, tableName, rowNumbers, incremental)
    } else {
        redshift.saveDataFrame(dataFrame, tableName, rowNumbers, incremental, forcedColumnTypes)
    }
    newRow <- data.frame(tableName, stringsAsFactors = FALSE)
    kv <- rbind(wrapper.tableNames, newRow)
    assign('wrapper.tableNames', kv, envir = .GlobalEnv)
}


#' Helper function to print timestamp with each message
#' @param msg Arbitrary msg or printable object
#' @return void
wrapper.print <- function(msg) {
    if (is.character(msg)) {
        print(paste(format(
            Sys.time(), "%Y-%m-%d %H:%M:%OS3"), ':',
            msg))
    } else {
        print(format(
            Sys.time(), "%Y-%m-%d %H:%M:%OS3"))
        print(msg)
    }
}


# process command line
wrapper.run()
