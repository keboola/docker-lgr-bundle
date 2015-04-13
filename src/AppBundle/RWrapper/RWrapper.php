<?php

namespace AppBundle\RWrapper;

use Monolog\Logger;
use Symfony\Component\Process\Process;

/**
 * Class which does the actual execution of R script.
 */
class RWrapper
{
    /**
     * Network port to communicate with Amazon Redshift database.
     */
    const REDSHIFT_DB_PORT = 5439;

    /**
     * Directory with R Script modules.
     */
    const R_SCRIPT_DIR = 'RScripts';

    /**
     * Directory with DB driver libraries.
     */
    const LIB_DIR = 'Resources';

    /**
     * Symfony logger.
     *
     * @var Logger
     */
    private $logger;

    /**
     * Database credentials
     * @var array
     */
    private $credentials;


    /**
     * Constructor.
     *
     * @param Logger $log Logger instance.
     * @param array $credentials Array of database credentials
     */
    public function __construct(Logger $log, array $credentials)
    {
        $this->logger = $log;
        $this->credentials = $credentials;
    }


    /**
     * Run an R script.
     * @param string $scriptFileName PathName of R script to run.
     * @param string $rScriptExecutable PathName of R (RScript) executable.
     * @param string $dbDriver FileNAme of DB driver to use.
     * @param string $workDirectory Pathname of temporary working directory.
     * @param string $sourceTable Name of source table (without schema).
     * @param string $params Arbitrary JSON parameters passed to the script.
     * @param bool $debug True to enable debugging mode, false to disable.
     * @throws \RuntimeException In case of error.
     */
    public function run($scriptFileName, $rScriptExecutable, $workDirectory, $dbDriver, $sourceTable, $params, $debug)
    {
        $this->logger->debug("Attempting to run script $scriptFileName");

        // quote the path
        $rScriptPath = '"' . $rScriptExecutable . '"';
        $dbDriver = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR .
            self::LIB_DIR . DIRECTORY_SEPARATOR . $dbDriver;
        $rWrapper = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR .
            self::R_SCRIPT_DIR . DIRECTORY_SEPARATOR . 'wrapper.R';
        $dbConn = "jdbc:postgresql://{$this->credentials['hostname']}:" .
            self::REDSHIFT_DB_PORT . "/{$this->credentials['db']}";

        $params = json_encode($params);
        if (strncasecmp(PHP_OS, 'WIN', 3) == 0) {
            // on windows? escapshellarg() replaces double quote with an empty space
            $params = '"' . str_replace('"', '\"', $params) . '"';
        } else {
            $params = escapeshellarg($params);
        }

        $this->logger->info("Running R-script $scriptFileName, this may take some time.");
        $commandLine =
            $rScriptPath . " --vanilla " . // vanilla = do not load and store the session (i.e. no global state)
            realpath($rWrapper) . " " . // R Wrapper - this is being executed
            realpath($scriptFileName) . " " . // Actual R module which we want to run
            escapeshellarg(realpath($dbDriver)) . " " .
            escapeshellarg($dbConn) . " " .
            escapeshellarg($this->credentials['user']) . " " .
            escapeshellarg($this->credentials['password']) . " " .
            escapeshellarg($this->credentials['schema']) . " " .
            escapeshellarg($workDirectory) . " " .
            escapeshellarg($sourceTable) . " " .
            $params . " " .
            $debug . " ";

        $this->logger->debug(
            "Executing command line " . str_replace($this->credentials['password'], '*****', $commandLine)
        );
        // execute the command line
        $process = new Process($commandLine);
        $process->setTimeout(0);
        $process->run();
        $output = $process->getOutput();
        $return = $process->getExitCode();

        // process the results
        $this->logger->debug("RETURN: " . var_export($return, true));
        $this->logger->debug("OUTPUT: " . var_export($output, true));
        if ($return != 0) {
            if ($debug) {
                if (file_exists($workDirectory . DIRECTORY_SEPARATOR . 'debug.log')) {
                    $ret = file($workDirectory . DIRECTORY_SEPARATOR . 'debug.log');
                    $this->logger->error("The command failed with the following errors:");
                    foreach ($ret as $line) {
                        $this->logger->error($line);
                    }
                    $ret = implode(" ", $ret);
                } else {
                    $output2 = $process->getErrorOutput();
                    if ($output2) {
                        $output .= "Additional error: " . $output2;
                    }
                    $this->logger->error("The command failed with the following errors:");
                    foreach (explode("\n", $output) as $line) {
                        $this->logger->error($line);
                    }
                    $ret = $output;
                }
            } else {
                $output2 = $process->getErrorOutput();
                if ($output2) {
                    $output .= "Additional error: " . $output2;
                }
                $this->logger->error("The command failed with message " . $output);
                $ret = $output;
                if ((stripos($ret, 'unable to load shared object') !== false)
                    && (stripos($ret, 'rJava') !== false)
                ) {
                    $ret = 'Cannot load Java JRE, verify that JAVA_HOME path is correct. Stack: ' . $ret;
                }
            }
            throw new \RuntimeException($ret);
        }
    }
}
