# This script can be opened from RStudio to develop and test a LGR module.
# --- Begin settings --- 

# R module parameters:
moduleToExecute = "ExampleModule.R"
sourceTable = "fooBar"
params = '{}'

# Database connection:
dbUser = "foo"
dbPassword = "bar"
dbSchema = "baz"
jdbcUrl = "jdbc:postgresql://server:5439/dbName"

# Run-time settings
debug = 1
workingDir = "~/tmp"
jdbcDriver = "~/luckyguess-r/Lib/postgresql-9.1-901.jdbc4.jar"

# --- End of settings --- 

# Detach all packages except for the core ones
packages <- search()
packages <- packages[grep(pattern = 'package:', packages)]
packages <- packages[-grep(pattern = 'base|utils|stats|methods|datasets|graphics|grDevices', packages)]
lapply(packages, function(package) {
  detach(package, character.only = TRUE)
})

# flush left-over sinks, if any
for(i in seq_len(sink.number())){
  sink(NULL)
}

# clear old debug file, if any
debugFileName <- file.path(workingDir, "debug.log");
if (file.exists(debugFileName)) {
  unlink(debugFileName)
}

# set this to base path of LGR RScripts, end path with slash
basePath <- "D:/wwwroot/luckyguess-r/RScripts/"
setwd(basePath)
wrapper.path <- basePath

tryCatch(source(paste0(basePath, "wrapper.R"), chdir = TRUE), error = function(e) {})

# provide credentials to an existing TRANSFORMATION sandbox, 
# working directory and other parameters for module

tryCatch({
    wrapper.run(
      moduleToExecute = moduleToExecute,
      jdbcDriver = jdbcDriver,
      jdbcUrl = jdbcUrl,
      dbUser = dbUser,
      dbPassword = dbPassword,
      dbSchema = dbSchema,
      workingDir = workingDir,
      sourceTable = sourceTable,
      params = params,
      debug = debug
    )
  }, finally = {
    if (file.exists(debugFileName)) {
      ddebugFile <- file(debugFileName)
      logData <- readLines(con = ddebugFile)
      cat(paste(logData, collapse = "\n"))
      close(ddebugFile)
    }
  }
)

# Note that if you are running RStudio on Windows a message may popup asking whether to reload R environment. 
# It does not really matter if you do or not, because this script is self contained. But the message itself sometimes
# causes an error like:
# > install.packages(pkgs = eval(packages), lib = eval(wrapper.workingDir), 
# > Error in install.packages : zip file ?package:ggplot2? not found
# to popup, which is not a problem because it appears after the script has been processed. It seems to be a bug
# in RStudio.

