<?php

namespace AppBundle\Command;

use AppBundle\RWrapper\RWrapper;
use Keboola\Provisioning\Client as ProvisioningClient;
use \Keboola\StorageApi\Client as StorageApiClient;
use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Yaml\Yaml;

/**
 * Class RunCommand executes the main command of the bundle - runs an R script
 *  provided in the configuration parameters.
 *
 * @package Keboola\DockerLGRBundle\Command
 */
class RunCommand extends ContainerAwareCommand
{
    /**
     * Filesystem handler.
     *
     * @var Filesystem
     */
    private $fs;

    /**
     * Logger interface.
     *
     * @var Logger
     */
    private $logger;

    /**
     * Provisioning client.
     *
     * @var ProvisioningClient
     */
    private $provisioning;

    /**
     * R script parameters.
     *
     * @var array
     */
    private $scriptParams;

    /**
     * Name of table (without schema) in transformation bucket which contains source data.
     *
     * @var string
     */
    private $sourceTable;

    /**
     * File tags which will be added to the resulting files.
     *
     * @var array
     */
    private $fileTags;

    /**
     * R script contents.
     *
     * @var string
     */
    private $scriptContent;

    /**
     * True if debugging mode is turned on.
     *
     * @var bool
     */
    private $debug;

    /**
     * Data directory.
     *
     * @var string
     */
    private $dataDir;


    /**
     * @inheritdoc
     */
    protected function configure()
    {
        $this
            ->setName('lgr:run')
            ->setDescription('Run an R script provided by configuration in configuration directory.')
            ->addOption(
                'data',
                'd',
                InputOption::VALUE_REQUIRED,
                'Location of the data directory with configuration'
            );
    }


    /**
     * Initializes the command just after the input has been validated.
     *
     * This is mainly useful when a lot of commands extends one main command
     * where some things need to be initialized based on the input arguments and options.
     *
     * @param InputInterface $input An InputInterface instance
     * @param OutputInterface $output An OutputInterface instance
     * @throws \Exception
     */
    protected function initialize(InputInterface $input, OutputInterface $output)
    {
        $this->fs = new Filesystem();

        // get Storage API token and initialize client
        $env = getenv('KBC_TOKENID');
        if (!empty($env)) {
            $token = $env;
        } else {
            throw new \InvalidArgumentException(
                "Storage API token must be provided in environment variable KBC_TOKENID. " .
                "Available environment variables are: ".
                implode(',', array_merge(array_keys($_SERVER), array_keys($_ENV)))
            );
        }
        $storageClient = new StorageApiClient(
            [
                'token' => $token,
                'url' => $this->getContainer()->getParameter('storage_api_url')
            ]
        );

        // get Storage API Run ID
        $env = getenv('KBC_RUNID');
        if (!empty($env)) {
            $runId = $env;
            $storageClient->setRunId($runId);
        } else {
            $runId = '';
        }

        // set logger to save messages to Storage
        $this->logger = $this->getContainer()->get('logger');
        $logHandler = $this->getContainer()->get('app_bundle.event_handler');
        $logHandler->setStorageApiClient($storageClient);
        $this->logger->debug("Token set to: " . $token);
        $this->logger->debug("Run ID set to: " . $runId);

        try {
            $this->provisioning = new ProvisioningClient('redshift', $token, $runId);

            // read configuration file injected by Docker (keboola\docker-bundle)
            $this->dataDir = $input->getOption('data');
            if (!file_exists($this->dataDir) || !is_dir($this->dataDir)) {
                throw new \InvalidArgumentException("Data directory does not exist or is not directory.");
            }
            $this->dataDir = realpath($this->dataDir);
            $this->logger->debug("Using directory: $this->dataDir");

            $configFileName = $this->dataDir . DIRECTORY_SEPARATOR . "config.yml";
            if (!file_exists($configFileName)) {
                throw new \InvalidArgumentException("config.yml is not present in data directory.");
            }
            $config = Yaml::parse(file_get_contents($configFileName));

            // verify and process configuration parameters
            if (isset($config['parameters']['debug'])) {
                $this->debug = boolval($config['parameters']['debug']);
            } else {
                $this->debug = $this->getContainer()->getParameter('kernel.environment');
            }
            $this->logger->debug("Debug mode set to : " . (int)($this->debug));

            if (isset($config['fileTags']) && is_array($config['fileTags'])) {
                $this->fileTags = $config['fileTags'];
            } else {
                $this->fileTags = [];
            }
            $this->logger->debug("Output file tags are set to: " . print_r($this->fileTags, true));

            if (!empty($config['parameters']['sourceTable'])) {
                $this->sourceTable = $config['parameters']['sourceTable'];
            } else {
                throw new \InvalidArgumentException("Source table must be provided in configuration.");
            }
            $this->logger->debug("Source table is set to: " . $this->sourceTable);

            if (!empty($config['parameters']['scriptParameters'])) {
                $this->scriptParams = $config['parameters']['scriptParameters'];
            } else {
                $this->scriptParams = [];
            }
            $this->logger->debug("Script parameters are set to: " . print_r($this->scriptParams, true));

            if (empty($config['parameters']['script'])) {
                throw new \InvalidArgumentException("Script content is empty.");
            }
            $this->scriptContent = '';
            if (is_array($config['parameters']['script'])) {
                foreach ($config['parameters']['script'] as $row) {
                    $this->scriptContent .= $row . "\n";
                }
            } else {
                $this->scriptContent = trim($config['parameters']['script']);
            }
            if (empty($this->scriptContent)) {
                throw new \InvalidArgumentException("Script content is empty.");
            }
        } catch (\InvalidArgumentException $e) {
            $this->logger->error("There was an error in input: " . $e->getMessage());
            throw $e;
        } catch (\Exception $e) {
            $this->logger->error("Application error.");
            $this->logger->debug("Application error: " . $e->getMessage());
            throw $e;
        }
    }


    /**
     * @inheritdoc
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {

        try {
            // create working directory
            $workingDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid("run-", true);
            if (!file_exists($workingDir) && !is_dir($workingDir)) {
                $this->fs->mkdir($workingDir);
            }

            // generate R script to run
            $scriptFileName = $workingDir . DIRECTORY_SEPARATOR . "script.R";
            $this->fs->dumpFile($scriptFileName, $this->scriptContent);

            // get database credentials
            $credentials = $this->provisioning->getCredentials('transformations');
            if (empty($credentials['credentials']) || empty($credentials['credentials']['hostname'])) {
                throw new \InvalidArgumentException("I cannot get credentials for Redshift database.");
            }
            // keep only the credentials
            $credentials = $credentials['credentials'];

            // initialize and run the R code
            $wrapper = new RWrapper($this->logger, $credentials);
            $wrapper->run(
                $scriptFileName,
                $this->getContainer()->getParameter('rScript'),
                $workingDir,
                $this->getContainer()->getParameter('dbDriver'),
                $this->sourceTable,
                $this->scriptParams,
                $this->debug
            );
            $this->logger->debug("R script has finished. I will now store the result files.");

            // save any files produced by the R script to the output
            $db = new \PDO(
                "pgsql:dbname={$credentials['db']};host={$credentials['hostname']};port=" . RWrapper::REDSHIFT_DB_PORT,
                $credentials['user'],
                $credentials['password']
            );
            // on errors raise exceptions
            $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            /* select each explicitly stored files, we do this rather than iterating over files in the
                working directory, because the working directory may contain a lot of garbage. */
            $stmt = $db->query("SELECT name, value FROM \"" . $credentials['schema'] . "\".\"r__file_names\";");
            while ($row = $stmt->fetch()) {
                // move file to output directory
                $this->logger->debug("I'm creating manifest for file: ".$row['value']);

                $this->fs->rename(
                    $workingDir . DIRECTORY_SEPARATOR . $row['value'],
                    $this->getOutDirectory() . $row['value']
                );
                // write manifest
                $manifest = [
                    'name' => $row['value'],
                    'is_public' => false,
                    'is_permanent' => true,
                    'notify' => false,
                    'tags' => array_merge($this->fileTags, [$row['name'], 'LuckyGuess'])
                ];
                $this->fs->dumpFile(
                    $this->getOutDirectory() . $row['value'] . '.manifest',
                    Yaml::dump($manifest)
                );
            }
            $this->logger->info("Everything finished.");
        } catch (\InvalidArgumentException $e) {
            $this->logger->error("There was an error in input: " . $e->getMessage());
        } catch (\Exception $e) {
            $this->logger->error("Application error.");
            $this->logger->debug("Application error: " . $e->getMessage());
        }
    }


    /**
     * Get path to output data directory.
     *
     * @return string Full path name.
     */
    private function getOutDirectory()
    {
        return $this->dataDir . DIRECTORY_SEPARATOR . 'out' . DIRECTORY_SEPARATOR . 'files' . DIRECTORY_SEPARATOR;
    }
}
