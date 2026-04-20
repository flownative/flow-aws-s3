<?php
declare(strict_types=1);

namespace Flownative\Aws\S3\Command;

/*
 * This script belongs to the package "Flownative.Aws.S3".
 */

use Aws\S3\BatchDelete;
use Aws\S3\S3Client;
use Flownative\Aws\S3\S3Target;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\Storage\StorageObject;

/**
 * S3 command controller for the Flownative.Aws.S3 package
 *
 * @Flow\Scope("singleton")
 */
class S3CommandController extends CommandController
{
    /**
     * @Flow\InjectConfiguration("profiles.default")
     * @var array
     */
    protected $s3DefaultProfile;

    /**
     * @Flow\Inject
     * @var ResourceManager
     */
    protected $resourceManager;

    /**
     * @var S3Client
     */
    private $s3Client;

    public function initializeObject()
    {
        $this->s3Client = new S3Client($this->s3DefaultProfile);
    }

    /**
     * Checks the connection
     *
     * This command checks if the configured credentials and connectivity allows for connecting with the S3 web service.
     *
     * By default, this command will run the "list buckets" operation on the S3 web service. If your IAM policy does
     * not allow listing buckets, you can pass a specific bucket through the "--bucket" argument. In that case, this
     * command will try to retrieve meta data only for that given bucket using the "head bucket" operation.
     *
     * @param string|null $bucket If specified, we try to connect by retrieving meta data for this specific bucket only
     * @param string $prefix
     * @return void
     */
    public function connectCommand(string $bucket = null, string $prefix = ''): void
    {
        try {
            if ($bucket !== null) {
                $this->s3Client->registerStreamWrapper();

                $this->outputLine('Access list of objects in bucket "%s" with key prefix "%s" ...', [$bucket, $prefix]);
                $this->s3Client->getPaginator('ListObjects', ['Bucket' => $bucket, 'Prefix' => $prefix]);

                $options = [
                    'Bucket' => $bucket,
                    'Body' => 'test',
                    'ContentLength' => 4,
                    'ContentType' => 'text/plain',
                    'Key' => $prefix . 'Flownative.Aws.S3.ConnectionTest.txt'
                ];
                $this->outputLine('Writing test object into bucket (arn:aws:s3:::%s/%s) ...', [$bucket, $options['Key']]);
                $this->s3Client->putObject($options);

                $this->outputLine('Deleting test object from bucket ...');
                $options = [
                    'Bucket' => $bucket,
                    'Key' => $prefix . 'Flownative.Aws.S3.ConnectionTest.txt'
                ];
                $this->s3Client->deleteObject($options);
            } else {
                $this->outputLine('Listing buckets ...');
                $this->s3Client->listBuckets();
            }
        } catch (\Exception $e) {
            $this->outputLine('<b>' . $e->getMessage() . '</b>');
            if ($bucket === null || $prefix === '') {
                $this->outputLine('Hint: Maybe your IAM policy restricts the user from listing all buckets. In that case, try using the "--bucket" and "--prefix" arguments.');
            }
            $this->quit(1);
        }
        $this->outputLine();
        $this->outputLine('OK');
    }

    /**
     * Displays a list of buckets
     *
     * This command outputs a list of all buckets from the currently configured S3
     * account.
     *
     * @return void
     */
    public function listBucketsCommand(): void
    {
        try {
            $result = $this->s3Client->listBuckets();
        } catch (\Exception $e) {
            $this->outputLine($e->getMessage());
            $this->quit(1);
        }

        if (count($result['Buckets']) === 0) {
            $this->outputLine('The account currently does not have any buckets.');
        }

        $tableRows = [];
        $headerRow = ['Bucket Name', 'Creation Date'];

        foreach ($result['Buckets'] as $bucket) {
            $tableRows[] = [$bucket['Name'], $bucket['CreationDate']];
        }

        $this->output->outputTable($tableRows, $headerRow);
    }

    /**
     * Removes all objects from a bucket
     *
     * This command deletes all objects (files) of the given bucket.
     *
     * @param string $bucket Name of the bucket
     * @return void
     */
    public function flushBucketCommand(string $bucket): void
    {
        try {
            $batchDelete = BatchDelete::fromListObjects($this->s3Client, ['Bucket' => $bucket]);
            $promise = $batchDelete->promise();
        } catch (\Exception $e) {
            $this->outputLine($e->getMessage());
            $this->quit(1);
        }
        $promise->wait();
        $this->outputLine('Successfully flushed bucket %s.', [$bucket]);
    }

    /**
     * Upload a file to a bucket
     *
     * This command uploads the file specified by <b>file</b> to the bucket
     * specified by <bucket>. The bucket must exist already in order to upload
     * the file.
     *
     * @param string $bucket Name of the bucket
     * @param string $file Full path leading to the file to upload
     * @param string $key Key to use for the uploaded object, for example "Coffee.jpg" or "MyPictures/Machines/Coffee.jpg". If not specified, the original filename is used.
     * @return void
     */
    public function uploadCommand(string $bucket, string $file, string $key = ''): void
    {
        if (!file_exists($file)) {
            $this->outputLine('The specified file does not exist.');
            $this->quit(1);
        }

        if ($key === '') {
            $key = basename($file);
        }

        try {
            $this->s3Client->putObject([
                'Key' => $key,
                'Bucket' => $bucket,
                'Body' => fopen('file://' . realpath($file), 'rb')
            ]);
        } catch (\Exception $e) {
            $this->outputLine('Could not upload %s to %s::%s – %s', [$file, $bucket, $key, $e->getMessage()]);
            $this->quit(1);
        }

        $this->outputLine('Successfully uploaded %s to %s::%s.', [$file, $bucket, $key]);
    }

    /**
     * Republish a collection
     *
     * This command forces publishing resources of the given collection by copying resources from the respective storage
     * to target bucket.
     *
     * @param string $collection Name of the collection to publish
     */
    public function republishCommand(string $collection = 'persistent'): void
    {
        $collectionName = $collection;
        $collection = $this->resourceManager->getCollection($collectionName);
        if (!$collection) {
            $this->outputLine('<error>The collection %s does not exist.</error>', [$collectionName]);
            exit(1);
        }

        $target = $collection->getTarget();
        if (!$target instanceof S3Target) {
            $this->outputLine('<error>The target defined in collection %s is not an S3 target.</error>', [$collectionName]);
            exit(1);
        }

        $this->outputLine('Republishing collection ...');
        $this->output->progressStart();
        try {
            foreach ($collection->getObjects() as $object) {
                /** @var StorageObject $object */
                $resource = $this->resourceManager->getResourceBySha1($object->getSha1());
                if ($resource) {
                    $target->publishResource($resource, $collection);
                }
                $this->output->progressAdvance();
            }
        } catch (\Exception $e) {
            $this->outputLine('<error>Publishing failed</error>');
            $this->outputLine($e->getMessage());
            $this->outputLine(get_class($e));
            exit(2);
        }
        $this->output->progressFinish();
        $this->outputLine();
    }

    /**
     * @param string $s3Object
     * @param string $keyPrefix
     * @param bool $removeUnregistered Remove objetcs from storage, that are not
     * @param bool $debug
     * @return void
     */
    public function diffS3ToLocalResourcesCommand(string $bucket, string $keyPrefix = '', bool $removeUnregistered = false, bool $debug = false)
    {
        $localMissingResources = 0;

        $objects = $this->s3Client->getIterator('ListObjects', [
            "Bucket" => $bucket,
            "Prefix" => $keyPrefix,
        ]);

        foreach ($objects as $s3Object) {
            $persistentObjectIdentifier = substr($s3Object['Key'], strlen($keyPrefix) + 1);
            $resource = $this->resourceManager->getResourceBySha1($persistentObjectIdentifier);
            $this->output->progressAdvance();

            if (!$resource instanceof PersistentResource) {
                $debug && $this->outputFormatted(sprintf('<error>S3 object with identifier <b>%s</b> (%s) is missing in the resource management<error>', $persistentObjectIdentifier, $s3Object['Key']));

                if ($removeUnregistered) {
                    $result = $this->s3Client->deleteObject([
                        'Bucket' => $bucket,
                        'Key' => $s3Object['Key']
                    ]);

                    if ($result['@metadata']['statusCode'] >= 200 && $result['@metadata']['statusCode'] <= 300) {
                        $this->outputLine(sprintf('<success>Successfully deleted object with key "%s"', $s3Object['Key']));
                    } else {
                        $this->outputLine(sprintf('<error>Error while deleting object with key "%s". Details: "%s"', $s3Object['Key'], json_encode($result)));
                    }
                }

                $localMissingResources++;
            }
        }

        $this->output->progressFinish();
        $this->outputLine();
        $this->outputLine(sprintf('%s resources are missing in the resource management.', $localMissingResources));
    }
}
