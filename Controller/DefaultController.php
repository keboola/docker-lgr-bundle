<?php

namespace Keboola\LuckyGuessRBundle\Controller;

use Keboola\LuckyGuessRBundle\MainTransformation;
use Keboola\Syrup\Controller\ApiController;
use Keboola\Syrup\Exception\UserException;

class DefaultController extends ApiController
{
    /**
     * Handler for executing a R script.
     *
     * @param \Symfony\Component\HttpFoundation\Request $request HTTP Request.
     * @return \Symfony\Component\HttpFoundation\JsonResponse HTTP response.
     * @throws UserException In case of missing parameters.
     */
    public function execAction(\Symfony\Component\HttpFoundation\Request $request)
    {
        set_time_limit(0);

        // Get params from request
        $params = $this->getPostJson($request);

        // Validate parameters
        if (empty($params['script'])) {
            throw new UserException("You need to provide value for parameter 'script' in request.");
        }
        $script = trim($params['script']);
        if (empty($params['source'])) {
            throw new UserException("You need to provide value for parameter 'source' in request.");
        }
        $source = trim($params['source']);
        if (empty($params['parameters'])) {
            $parameters = '{}';
        } else {
            $parameters = $params['parameters'];
        }

        if (empty($params['fileTags']) || !is_array($params['fileTags'])) {
            $fileTags = [];
        } else {
            $fileTags = $params['fileTags'];
        }

        $trans = new MainTransformation($this->storageApi, $this->logger, $this->container);

        $data = $trans->run($script, $source, $parameters, $fileTags, !empty($params['debug']));
        return $this->createJsonResponse($data, 200);
    }


    /**
     * Handler for getting list of available modules.
     *
     * @return \Symfony\Component\HttpFoundation\JsonResponse HTTP response.
     */
    public function listAction()
    {
        $trans = new MainTransformation($this->storageApi, $this->logger, $this->container);

        $dir = __DIR__ . DIRECTORY_SEPARATOR . '..' . DIRECTORY_SEPARATOR .
            MainTransformation::R_SCRIPT_DIR . DIRECTORY_SEPARATOR;
        $data = [];
        foreach (glob($dir . "*.R") as $fullName) {
            $fileName = basename($fullName, ".R");
            if (!in_array($fileName, array('wrapper', 'redshift', 'RStudioRunner', 'wrapperParams'))) {
                $data[$fileName] = array(
                    "name" => $fileName,
                    "parameters" => $trans->getParameters($fileName)
                );
            }
        }


        return $this->createJsonResponse($data, 200);
    }
}
