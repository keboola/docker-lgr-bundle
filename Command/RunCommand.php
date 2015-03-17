<?php

namespace Keboola\DockerLGRBundle\Command;

use Keboola\DockerLGRBundle\RWrapper\RWrapper;
use Keboola\Provisioning\Client;
use Keboola\Syrup\Exception\UserException;
use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
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
     * @inheritdoc
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        /** @var Logger $logger */
        $logger = $this->getContainer()->get('logger');
        $fs = new Filesystem();
        $dataDir = realpath($input->getOption('data'));
        $logger->debug("Using directory: $dataDir");
        $config = Yaml::parse(file_get_contents($dataDir . DIRECTORY_SEPARATOR . "config.yml"));

        // verify config parameters
        if (isset($config['parameters']['debug'])) {
            $debug = boolval($config['parameters']['debug']);
        } else {
            $debug = $this->getContainer()->getParameter('kernel.environment');
        }
        $logger->debug("Debug mode set to : " . (int)$debug);

        if (isset($config['fileTags']) && is_array($config['fileTags'])) {
            $fileTags = $config['fileTags'];
        } else {
            $fileTags = [];
        }
        $logger->debug("Output file tags set to: " . print_r($fileTags, true));

        if (!empty($config['parameters']['sourceTable'])) {
            $sourceTable = $config['parameters']['sourceTable'];
        } else {
            throw new UserException("Source table must be provided in configuration.");
        }
        $logger->debug("Source table set to: " . $sourceTable);

        if (!empty($config['parameters']['scriptParams'])) {
            $scriptParams = $config['parameters']['scriptParams'];
        } else {
            $scriptParams = [];
        }
        $logger->debug("Script parameters set to: " . print_r($scriptParams, true));

        if (!empty($config['token'])) {
            $token = $config['token'];
        } else {
            throw new UserException("Storage API token must be provided in configuration.");
        }
        $logger->debug("Token set to: " . $token);

        if (!empty($config['runId'])) {
            $runId = $config['runId'];
        } else {
            $runId = '';
        }
        $logger->debug("Run ID set to: " . $runId);

        // generate R script to run
        $scriptContent = '';
        $scriptFileName = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR .
            RWrapper::R_SCRIPT_DIR . DIRECTORY_SEPARATOR . "script.R";
        foreach ($config["parameters"]["script"] as $row) {
            $scriptContent .= $row . "\n";
        }
        $fs->dumpFile($scriptFileName, $scriptContent);

        // create working directory
        $this->getContainer()->get('syrup.temp')->initRunFolder();
        $workingDir = $this->getContainer()->get('syrup.temp')->getTmpFolder();

        // get database credentials
        $provisioning = new Client('redshift', $token, $runId);
        $credentials = $provisioning->getCredentials('transformations');
        if (empty($credentials['credentials']) || empty($credentials['credentials']['hostname'])) {
            throw new UserException("I cannot get credentials for Redshift database.");
        }
        // keep only the credentials
        $credentials = $credentials['credentials'];

        // initialize and run the R code
        $wrapper = new RWrapper($logger, $credentials);
        $wrapper->run(
            $scriptFileName,
            $this->getContainer()->getParameter('config.rScript'),
            $this->getContainer()->getParameter('config.dbDriver'),
            $workingDir,
            $sourceTable,
            $scriptParams,
            $debug
        );
        $logger->debug("Run finished, storing files");

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
            $fs->rename(
                $workingDir . DIRECTORY_SEPARATOR . $row['value'],
                $dataDir . DIRECTORY_SEPARATOR . 'out' . DIRECTORY_SEPARATOR . $row['value']
            );
            // write manifest
            $manifest = [
                'name' => $row['value'],
                'is_public' => 0,
                'is_permanent' => 1,
                'notify' => 0,
                'tags' => array_merge($fileTags, [$row['name'], 'LuckyGuess'])
            ];
            $fs->dumpFile(
                $dataDir . DIRECTORY_SEPARATOR . 'out' . DIRECTORY_SEPARATOR . $row['value'] . '.manifest',
                Yaml::dump($manifest)
            );
        }
    }
}
