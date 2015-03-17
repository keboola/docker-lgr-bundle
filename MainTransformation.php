<?php

namespace Keboola\LuckyGuessRBundle;

use Keboola\StorageApi\Client;
use Keboola\Syrup\Exception\UserException;
use Monolog\Logger;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 */
class MainTransformation
{
    /**
     * Directory with R Script modules.
     */
    const R_SCRIPT_DIR = 'RScripts';

    /**
     * Directory with DB driver libraries.
     */
    const LIB_DIR = 'Lib';

    /**
     * Logger instance.
     *
     * @var Logger
     */
    private $log;

    /**
     * Symfony service container.
     *
     * @var ContainerInterface
     */
    private $container;

    /**
     * Storage API client.
     *
     * @var Client
     */
    private $storageApi;


    /**
     * Constructor.
     *
     * @param Client $storageApi Storage Api client instance.
     * @param Logger $log Logger instance.
     * @param ContainerInterface $container Symfony service container.
     */
    public function __construct(Client $storageApi, Logger $log, ContainerInterface $container)
    {
        $this->log = $log;
        $this->container = $container;
        $this->storageApi = $storageApi;
    }


    /**
     * Run an R script.
     * @param string $script Name of R script to run (without extension).
     * @param string $sourceTable Name of source table (without schema).
     * @param string $params Arbitrary JSON parameters passed to the script.
     * @param array $fileTags List of file tags.
     * @param bool $debug True to enable debugging mode, false to disable.
     * @return array Array with item 'result' containing raw result of the R script. In
     *  most cases, this contains nothing useful as the results are stored directly in DB.
     * @throws UserException
     */
    public function run($script, $sourceTable, $params, array $fileTags, $debug)
    {
        $this->log->info("Attempting to run script $script");

        // quote the path
        $rScriptPath = '"' . $this->container->getParameter('config.rScript') . '"';
        $dbDriver = __DIR__ . DIRECTORY_SEPARATOR . self::LIB_DIR .
            DIRECTORY_SEPARATOR . $this->container->getParameter('config.dbDriver');

        $rWrapper = __DIR__ . DIRECTORY_SEPARATOR . self::R_SCRIPT_DIR . DIRECTORY_SEPARATOR . 'wrapper.R';
        # verify that the file to run really exists, otherwise wrapper will fail miserably
        $rFile = __DIR__ . DIRECTORY_SEPARATOR . self::R_SCRIPT_DIR . DIRECTORY_SEPARATOR . $script . '.R';
        if (!preg_match('#[a-z0-9_\.-]#i', $script) || !file_exists($rFile)) {
            throw new UserException("I cannot find R script $rFile, verify that 'script' is an existing R script.");
        }

        // get database credentials
        $provisioning = new \Keboola\Provisioning\Client(
            'redshift',
            $this->storageApi->getTokenString(),
            $this->storageApi->getRunId()
        );
        $credentials = $provisioning->getCredentials('transformations');
        if (empty($credentials['credentials']) || empty($credentials['credentials']['hostname'])) {
            throw new UserException("I cannot get credentials for Redshift database.");
        }
        // keep only the credentials
        $credentials = $credentials['credentials'];

        $dbPort = 5439;
        $dbServer = $credentials['hostname'];
        $dbName = $credentials['db'];
        $dbConn = "jdbc:postgresql://{$dbServer}:{$dbPort}/{$dbName}";
        $dbUser = $credentials['user'];
        $dbPass = $credentials['password'];
        $dbSchema = $credentials['schema'];

        // create working directory
        $this->container->get('syrup.temp')->initRunFolder();
        $workingDir = $this->container->get('syrup.temp')->getTmpFolder();

        $params = json_encode($params);
        if (strncasecmp(PHP_OS, 'WIN', 3) == 0) {
            // on windows? escapshellarg() replaces double quote with an empty space
            $params = '"' . str_replace('"', '\"', $params) . '"';
        } else {
            $params = escapeshellarg($params);
        }

        if ($debug || ($this->container->getParameter('kernel.environment') == 'dev')) {
            $debug = 1;
        } else {
            $debug = 0;
        }

        $this->log->info("Running R-script $script, this may take some time.");
        $commandLine =
            $rScriptPath . " --vanilla " . // vanilla = do not load and store the session (i.e. no global state)
            $rWrapper . " " . // R Wrapper - this is being executed
            $script . ".R " . // Actual R module which we want to run
            escapeshellarg($dbDriver) . " " .
            escapeshellarg($dbConn) . " " .
            escapeshellarg($dbUser) . " " .
            escapeshellarg($dbPass) . " " .
            escapeshellarg($dbSchema) . " " .
            escapeshellarg($workingDir) . " " .
            escapeshellarg($sourceTable) . " " .
            $params . " " .
            $debug . " ";

        $this->log->debug("Executing command line " . str_replace($dbPass, '*****', $commandLine));
        // execute the command line
        $process = new  \Symfony\Component\Process\Process($commandLine);
        $process->setTimeout(0);
        $process->run();
        $output = $process->getOutput();
        $return = $process->getExitCode();

        // process the results
        $this->log->debug("RETURN: " . var_export($return, true));
        $this->log->debug("OUTPUT: " . var_export($output, true));
        switch ($return) {
            case 0:
                $this->log->info("R-script successful, intermediate tables: " . $output);
                $ret = explode(",", $output);
                $ret = array_map('trim', $ret);
                break;
            default:
                if ($debug) {
                    if (file_exists($workingDir . DIRECTORY_SEPARATOR . 'debug.log')) {
                        $ret = file($workingDir . DIRECTORY_SEPARATOR . 'debug.log');
                        $this->log->error("The command failed with the following errors:");
                        foreach ($ret as $line) {
                            $this->log->error($line);
                        }
                        $ret = implode(" ", $ret);
                    } else {
                        $output2 = $process->getErrorOutput();
                        if ($output2) {
                            $output .= "Additional error: " . $output2;
                        }
                        $this->log->error("The command failed with the following errors:");
                        foreach (explode("\n", $output) as $line) {
                            $this->log->error($line);
                        }
                        $ret = $output;
                    }
                } else {
                    $output2 = $process->getErrorOutput();
                    if ($output2) {
                        $output .= "Additional error: " . $output2;
                    }
                    $this->log->error("The command failed with message " . $output);
                    $ret = $output;
                    if ((stripos($ret, 'unable to load shared object') !== false)
                        && (stripos($ret, 'rJava') !== false)
                    ) {
                        $ret = 'Cannot load Java JRE, verify that JAVA_HOME path is correct. Stack: ' . $ret;
                    }
                }
                throw new UserException($ret);
                break;
        }

        // save any files produced by the R script to SAPI
        $db = new \PDO(
            "pgsql:dbname={$dbName};host={$dbServer};port={$dbPort}",
            $dbUser,
            $dbPass
        );
        // on errors raise exceptions
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // prepare update statement to store SAPI ID with each file
        $uStmt = $db->prepare(
            "UPDATE " . $dbSchema . ".r__file_names SET id = :id WHERE name = :name AND value = :value;"
        );

        $stmt = $db->query("SELECT * FROM " . $dbSchema . ".r__file_names;");
        while ($row = $stmt->fetch()) {
            // upload file to SAPI
            $options = new \Keboola\StorageApi\Options\FileUploadOptions();
            $options->setIsPermanent(true);
            $options->setIsPublic(false);
            $options->setIsSliced(false);
            $options->setNotify(false);
            $tags = array($row['name'], $script, 'LuckyGuess');
            foreach ($fileTags as $fileTag) {
                $tags[] = trim($fileTag);
            }
            $options->setTags($tags);
            $fileName = $row['value'];
            $id = $this->storageApi->uploadFile($workingDir . DIRECTORY_SEPARATOR . $fileName, $options);

            // store it's ID in the file table
            $uStmt->bindValue(':name', $row['name']);
            $uStmt->bindValue(':value', $row['value']);
            $uStmt->bindValue(':id', $id);
            $uStmt->execute();
        }

        return array("result" => $ret);
    }


    public function getParameters($script)
    {
        $this->log->info("Attempting to run script $script");

        // quote the path
        $rScriptPath = '"' . $this->container->getParameter('config.rScript') . '"';
        $rWrapper = __DIR__.DIRECTORY_SEPARATOR.self::R_SCRIPT_DIR.DIRECTORY_SEPARATOR.'wrapperParams.R';

        # verify that the file to run really exists, otherwise wrapper will fail miserably
        $rFile = __DIR__.DIRECTORY_SEPARATOR.self::R_SCRIPT_DIR.DIRECTORY_SEPARATOR.$script.'.R';
        if (!preg_match('#[a-z0-9_\.-]#i', $script) || !file_exists($rFile)) {
            throw new UserException("I cannot find R script $rFile, verify that 'script' is an existing R script.");
        }
        $rFile = str_replace(DIRECTORY_SEPARATOR, '/', $rFile);

        $commandLine =
            $rScriptPath." --vanilla ". // vanilla = do not load and store the session (i.e. no global state)
            $rWrapper." ".
            $rFile." ".
            "2>&1" // redirect error output to stdout so that exec() catches it
        ;

        $this->log->debug("Executing command line ".$commandLine);
        exec($commandLine, $output, $return);
        $this->log->debug("RETURN: ".var_export($return, true));
        $this->log->debug("OUTPUT: ".var_export($output, true));
        $this->log->debug("Execution done");
        switch ($return) {
            case 0:
                $this->log->info("RScript successful: ".implode(" ", $output));
                $ret = $output;
                break;
            default:
                $ret = implode(" ", $output);
                $this->log->error("The command failed with message: ".$ret);
                throw new UserException($ret);
                break;
        }
        $parameters = [];
        foreach ($ret as $line) {
            $components = explode("=>", $line);
            if (count($components) != 2) {
                $this->log->warn("Invalid output line: $line");
                continue;
            }
            $name = trim($components[0]);
            $value = trim($components[1]);
            if (isset($parameters[$name]) || ($name == 'packages')) {
                if (isset($parameters[$name]) && !is_array($parameters[$name])) {
                    $parameters[$name] = array($parameters[$name]);
                }
                $parameters[$name][] = $value;
            } else {
                $parameters[$name] = trim($value);
            }
        }

        return $parameters;
    }
}
