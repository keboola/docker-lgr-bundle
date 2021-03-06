# main module function
module.run <- function() {
	# access parameters and save simple values
	wrapper.saveValue("paramNumber", wrapper.params$paramNumber);
	wrapper.saveValue("columnName", wrapper.params$paramNested$columnName);
	wrapper.saveValue("columnFoo", wrapper.params$paramNested$columnFoo);
	wrapper.saveValue("columnBar", wrapper.params$paramNested$columnBar);

	# save tables
	wrapper.saveValue("key2", "foo", 1);
	wrapper.saveValue("key3", "bar", 1);

	result <- data.frame(
        some_string = c("this", "is", "an", "example"),
        some_number = c(1, 2, 5, 42)
    )
	wrapper.saveDataFrame(result, "r_example_results", rowNumbers = TRUE)

	fileName <- 'exampleGraph.png'
	png(file.path(wrapper.workingDir, fileName), width = 400, height = 400)
	plot(result)
	# write to file
	dev.off()

	wrapper.saveFileName('ExampleModule', fileName)
    TRUE
}


# declare parameters required by the module
module.parameters <- function() {
    # list is in form "parameterName" = "dataType"
    # - "dataType can be one of "character", "integer", "double", "lg_column"
    # - nested parameters are defined with dot notation
    # - item 'packages' is a character vector of packges required to be loaded
    list(
        "paramNumber" = "integer",
        "paramNested.columnName" = "lg_column",
        "paramNested.columnFoo" = "double",
        "paramNested.columnBar" = "character",
        "packages" = c("leaps")
    )
}
