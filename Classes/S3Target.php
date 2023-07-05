<?php
declare(strict_types=1);

namespace Flownative\Aws\S3;

/*
 * This script belongs to the package "Flownative.Aws.S3".
 */

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use GuzzleHttp\Psr7\Uri;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\ResourceManagement\CollectionInterface;
use Neos\Flow\ResourceManagement\Exception;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\Publishing\MessageCollector;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\ResourceMetaDataInterface;
use Neos\Flow\ResourceManagement\Storage\StorageInterface;
use Neos\Flow\ResourceManagement\Storage\StorageObject;
use Neos\Flow\ResourceManagement\Target\TargetInterface;
use Psr\Log\LoggerInterface;

/**
 * A resource publishing target based on Amazon S3
 */
class S3Target implements TargetInterface
{
    /**
     * The ACL when uploading a file
     *
     * @Flow\InjectConfiguration(package="Flownative.Aws.S3", path="profiles.default.acl")
     * @var string
     */
    protected $acl;

    /**
     * The default ACL
     * @Flow\InjectConfiguration(package="Flownative.Aws.S3", path="profiles.default.acl")
     * @var string
     */
    protected $defaultAcl;

    /**
     * Name which identifies this resource target
     *
     * @var string
     */
    protected $name;

    /**
     * Name of the S3 bucket which should be used for publication
     *
     * @var string
     */
    protected $bucketName;

    /**
     * A prefix to use for the key of bucket objects used by this storage
     *
     * @var string
     */
    protected $keyPrefix;

    /**
     * @var string
     */
    protected $persistentResourceUriPattern = '';

    /**
     * CORS (Cross-Origin Resource Sharing) allowed origins for published content
     *
     * @var string
     */
    protected $corsAllowOrigin = '*';

    /**
     * @var string
     */
    protected $baseUri = '';

    /**
     * If TRUE (default), resources which are not anymore part of the storage will be removed
     * from the target as well. If set to FALSE, your target will only ever grow, never shrink.
     *
     * @var bool
     */
    protected $unpublishResources = true;

    /**
     * Internal cache for known storages, indexed by storage name
     *
     * @var array<StorageInterface>
     */
    protected $storages = [];

    /**
     * @var S3Client
     */
    protected $s3Client;

    /**
     * @Flow\InjectConfiguration("profiles.default")
     * @var array
     */
    protected $s3DefaultProfile;

    /**
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $systemLogger;

    /**
     * @Flow\Inject
     * @var MessageCollector
     */
    protected $messageCollector;

    /**
     * @Flow\Inject
     * @var ResourceManager
     */
    protected $resourceManager;

    /**
     * @var array
     */
    protected $existingObjectsInfo = [];

    /**
     * @var bool
     */
    protected $bucketIsPublic;

    /**
     * Constructor
     *
     * @param string $name Name of this target instance, according to the resource settings
     * @param array $options Options for this target
     * @throws Exception
     */
    public function __construct(string $name, array $options = [])
    {
        $this->name = $name;
        foreach ($options as $key => $value) {
            switch ($key) {
                case 'bucket':
                    $this->bucketName = (string)$value;
                break;
                case 'keyPrefix':
                    $this->keyPrefix = (string)$value;
                break;
                case 'corsAllowOrigin':
                    $this->corsAllowOrigin = (string)$value;
                break;
                case 'baseUri':
                    $this->baseUri = (string)$value;
                break;
                case 'unpublishResources':
                    $this->unpublishResources = (bool)$value;
                    break;
                case 'acl':
                    $this->acl = (string)$value;
                    break;
                case 'persistentResourceUris':
                    if (!is_array($value)) {
                        throw new Exception(sprintf('The option "%s" which was specified in the configuration of the "%s" resource S3Target is not a valid array. Please check your settings.', $key, $name), 1628259768);
                    }
                    foreach ($value as $uriOptionKey => $uriOptionValue) {
                        switch ($uriOptionKey) {
                            case 'pattern':
                                $this->persistentResourceUriPattern = (string)$uriOptionValue;
                            break;
                            default:
                                if ($uriOptionValue !== null) {
                                    throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource S3Target. Please check your settings.', $uriOptionKey, $name), 1628259794);
                                }
                        }
                    }
                break;
                default:
                    if ($value !== null) {
                        throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource S3Target. Please check your settings.', $key, $name), 1428928226);
                    }
            }
        }
    }

    /**
     * Initialize the S3 Client
     *
     * @return void
     */
    public function initializeObject(): void
    {
        $clientOptions = $this->s3DefaultProfile;

        $this->s3Client = new S3Client($clientOptions);
        $this->s3Client->registerStreamWrapper();
    }

    /**
     * Returns the name of this target instance
     *
     * @return string The target instance name
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Returns the S3 object key prefix
     *
     * @return string
     */
    public function getKeyPrefix(): string
    {
        return $this->keyPrefix;
    }

    /**
     * Returns the ACL when uploading a file
     *
     * @return string
     */
    public function getAcl()
    {
        return $this->acl ?? $this->defaultAcl;
    }

    /**
     * Publishes the whole collection to this target
     *
     * @param CollectionInterface $collection The collection to publish
     * @param callable|null $callback Function called after each resource publishing
     * @return void
     * @throws Exception
     * @throws \Neos\Flow\Exception
     * @throws \Exception
     */
    public function publishCollection(CollectionInterface $collection, callable $callback = null)
    {
        $storage = $collection->getStorage();

        if ($storage instanceof S3Storage && $storage->getBucketName() === $this->bucketName) {
            // TODO do we need to update the content-type on the objects?
            $this->systemLogger->debug(sprintf('Skipping resource publishing for bucket "%s", storage and target are the same.', $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
            return;
        }

        if ($this->existingObjectsInfo === []) {
            $requestArguments = [
                'Bucket' => $this->bucketName,
                'Prefix' => $this->keyPrefix
            ];

            do {
                $result = $this->s3Client->listObjectsV2($requestArguments);
                if ($result->get('Contents')) {
                    foreach ($result->get('Contents') as $item) {
                        $this->existingObjectsInfo[] = $item['Key'];
                    }
                }
                if ($result->get('IsTruncated')) {
                    $requestArguments['ContinuationToken'] = $result->get('NextContinuationToken');
                }
            } while ($result->get('IsTruncated'));
        }

        $potentiallyObsoleteObjects = array_fill_keys($this->existingObjectsInfo, true);

        if ($storage instanceof S3Storage) {
            $this->publishCollectionFromS3Storage($collection, $storage, $potentiallyObsoleteObjects, $callback);
        } else {
            foreach ($collection->getObjects($callback) as $object) {
                /** @var StorageObject $object */
                $this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
                $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
                $potentiallyObsoleteObjects[$objectName] = false;
            }
        }

        if ($this->unpublishResources === false) {
            $this->systemLogger->debug(sprintf('Skipping resource unpublishing from bucket "%s", because configuration option "unpublishResources" is FALSE.', $this->bucketName));
            return;
        }

        foreach (array_keys($potentiallyObsoleteObjects) as $relativePathAndFilename) {
            if (!$potentiallyObsoleteObjects[$relativePathAndFilename]) {
                continue;
            }
            $this->systemLogger->debug(sprintf('Deleted obsolete resource "%s" from bucket "%s"', $relativePathAndFilename, $this->bucketName));
            $this->s3Client->deleteObject([
                'Bucket' => $this->bucketName,
                'Key' => $this->keyPrefix . $relativePathAndFilename
            ]);
        }
    }

    /**
     * @param CollectionInterface $collection
     * @param S3Storage $storage
     * @param array $potentiallyObsoleteObjects
     * @param callable|null $callback
     */
    private function publishCollectionFromS3Storage(CollectionInterface $collection, S3Storage $storage, array &$potentiallyObsoleteObjects, callable $callback = null): void
    {
        foreach ($collection->getObjects($callback) as $object) {
            /** @var StorageObject $object */
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
            if (array_key_exists($objectName, $potentiallyObsoleteObjects)) {
                $this->systemLogger->debug(sprintf('The resource object "%s" (SHA1: %s) has already been published to bucket "%s", no need to re-publish', $objectName, $object->getSha1() ?: 'unknown', $this->bucketName));
                $potentiallyObsoleteObjects[$objectName] = false;
            } else {
                $this->copyObject(
                    function (StorageObject $object) use ($storage): string {
                        return $storage->getBucketName() . '/' . $storage->getKeyPrefix() . $object->getSha1();
                    },
                    function (StorageObject $object) use ($storage): string {
                        return $storage->getKeyPrefix() . $this->getRelativePublicationPathAndFilename($object);
                    },
                    $object
                );
            }
        }
    }

    /**
     * Returns the web accessible URI pointing to the given static resource
     *
     * @param string $relativePathAndFilename Relative path and filename of the static resource
     * @return string The URI
     */
    public function getPublicStaticResourceUri($relativePathAndFilename): string
    {
        if ($this->baseUri !== '') {
            return $this->baseUri . $relativePathAndFilename;
        }

        return $this->s3Client->getObjectUrl($this->bucketName, $this->keyPrefix . $relativePathAndFilename);
    }

    /**
     * Publishes the given persistent resource from the given storage
     *
     * @param PersistentResource $resource The resource to publish
     * @param CollectionInterface $collection The collection the given resource belongs to
     * @return void
     * @throws Exception
     * @throws \Neos\Flow\Exception
     * @throws \Exception
     */
    public function publishResource(PersistentResource $resource, CollectionInterface $collection)
    {
        $storage = $collection->getStorage();
        if ($storage instanceof S3Storage) {
            if ($storage->getBucketName() === $this->bucketName) {
                // to update the Content-Type the object must be copied to itself…
                $this->copyObject(
                    function (PersistentResource $resource) use ($storage): string {
                        return $this->bucketName . '/' . $storage->getKeyPrefix() . $resource->getSha1();
                    },
                    function (PersistentResource $resource) use ($storage): string {
                        return $storage->getKeyPrefix() . $resource->getSha1();
                    },
                    $resource
                );
                return;
            }

            $this->copyObject(
                function (PersistentResource $resource) use ($storage): string {
                    return urlencode($storage->getBucketName() . '/' . $storage->getKeyPrefix() . $resource->getSha1());
                },
                function (PersistentResource $resource): string {
                    return $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
                },
                $resource
            );
        } else {
            $sourceStream = $resource->getStream();
            if ($sourceStream === false) {
                $message = sprintf('Could not publish resource with SHA1 hash %s of collection %s because there seems to be no corresponding data in the storage.', $resource->getSha1(), $collection->getName());
                $this->messageCollector->append($message);
                return;
            }
            $this->publishFile($sourceStream, $this->getRelativePublicationPathAndFilename($resource), $resource);
        }
    }

    private function copyObject(\Closure $sourceBuilder, \Closure $targetBuilder, ResourceMetaDataInterface $resource): void
    {
        $source = $sourceBuilder($resource);
        $target = $targetBuilder($resource);

        $options = [
            'Bucket' => $this->bucketName,
            'CopySource' => $source,
            'ContentType' => $resource->getMediaType(),
            'MetadataDirective' => 'REPLACE',
            'Key' => $target
        ];
        if ($this->getAcl()) {
            $options['ACL'] = $this->getAcl();
        }
        try {
            $this->s3Client->copyObject($options);
            $this->systemLogger->debug(sprintf('Successfully published resource as object "%s" (SHA1: %s) by copying from "%s" to bucket "%s"', $target, $resource->getSha1() ?: 'unknown', $source, $this->bucketName));
        } catch (S3Exception $e) {
            $this->systemLogger->critical($e, LogEnvironment::fromMethodName(__METHOD__));
            $message = sprintf('Could not publish resource with SHA1 hash %s (source object: %s) through "CopyObject" because the S3 client reported an error: %s', $resource->getSha1(), $source, $e->getMessage());
            $this->messageCollector->append($message);
        }
    }

    /**
     * Unpublishes the given persistent resource
     *
     * @param PersistentResource $resource The resource to unpublish
     * @return void
     */
    public function unpublishResource(PersistentResource $resource)
    {
        if ($this->unpublishResources === false) {
            $this->systemLogger->debug(sprintf('Skipping resource unpublishing %s from bucket "%s", because configuration option "unpublishResources" is FALSE.', $resource->getSha1() ?: 'unknown', $this->bucketName));
            return;
        }

        $storage = $this->resourceManager->getCollection($resource->getCollectionName())->getStorage();
        if ($storage instanceof S3Storage && $storage->getBucketName() === $this->bucketName) {
            // Unpublish for same-bucket setups is a NOOP, because the storage object will already be deleted.
            $this->systemLogger->debug(sprintf('Skipping resource unpublishing %s from bucket "%s", because storage and target are the same.', $resource->getSha1() ?: 'unknown', $this->bucketName));
            return;
        }

        try {
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
            $this->s3Client->deleteObject([
                'Bucket' => $this->bucketName,
                'Key' => $objectName
            ]);
            $this->systemLogger->debug(sprintf('Successfully unpublished resource as object "%s" (SHA1: %s) from bucket "%s"', $objectName, $resource->getSha1() ?: 'unknown', $this->bucketName));
        } catch (\Exception $e) {
        }
    }

    /**
     * Returns the web accessible URI pointing to the specified persistent resource
     *
     * @param PersistentResource $resource Resource object or the resource hash of the resource
     * @return string The URI
     */
    public function getPublicPersistentResourceUri(PersistentResource $resource): string
    {

        if (empty($this->persistentResourceUriPattern)) {
            if (empty($this->baseUri)) {
                return $this->s3Client->getObjectUrl($this->bucketName, $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource));
            }

            return $this->baseUri . $this->encodeRelativePathAndFilenameForUri($this->getRelativePublicationPathAndFilename($resource));
        }

        $variables = [
            '{baseUri}' => $this->baseUri,
            '{bucketName}' => $this->bucketName,
            '{keyPrefix}' => $this->keyPrefix,
            '{sha1}' => $resource->getSha1(),
            '{filename}' => $resource->getFilename(),
            '{fileExtension}' => $resource->getFileExtension()
        ];

        $customUri = $this->persistentResourceUriPattern;
        foreach ($variables as $placeholder => $replacement) {
            $customUri = str_replace($placeholder, $replacement, $customUri);
        }

        // Let Uri implementation take care of encoding the Uri
        return (string)new Uri($customUri);
    }

    /**
     * Applies rawurlencode() to all path segments of the given $relativePathAndFilename
     *
     * @param string $relativePathAndFilename
     * @return string
     */
    protected function encodeRelativePathAndFilenameForUri(string $relativePathAndFilename): string
    {
        return implode('/', array_map('rawurlencode', explode('/', $relativePathAndFilename)));
    }

    /**
     * Publishes the specified source file to this target, with the given relative path.
     *
     * @param resource $sourceStream
     * @param string $relativeTargetPathAndFilename
     * @param ResourceMetaDataInterface $metaData
     * @throws \Exception
     */
    protected function publishFile($sourceStream, string $relativeTargetPathAndFilename, ResourceMetaDataInterface $metaData): void
    {
        $objectName = $this->keyPrefix . $relativeTargetPathAndFilename;
        $options = [
            'params' => [
                'ContentLength' => $metaData->getFileSize(),
                'ContentType' => $metaData->getMediaType()
            ]
        ];

        try {
            $this->s3Client->upload($this->bucketName, $objectName, $sourceStream, $this->getAcl() ?: null, $options);
            $this->systemLogger->debug(sprintf('Successfully published resource as object "%s" in bucket "%s" with SHA1 hash "%s"', $objectName, $this->bucketName, $metaData->getSha1() ?: 'unknown'));
        } catch (\Exception $e) {
            $this->systemLogger->debug(sprintf('Failed publishing resource as object "%s" in bucket "%s" with SHA1 hash "%s": %s', $objectName, $this->bucketName, $metaData->getSha1() ?: 'unknown', $e->getMessage()));
            if (is_resource($sourceStream)) {
                fclose($sourceStream);
            }
            throw $e;
        }
    }

    /**
     * Determines and returns the relative path and filename for the given Storage Object or Resource. If the given
     * object represents a persistent resource, its own relative publication path will be empty. If the given object
     * represents a static resources, it will contain a relative path.
     *
     * @param ResourceMetaDataInterface $object Resource or Storage Object
     * @return string The relative path and filename, for example "c828d0f88ce197be1aff7cc2e5e86b1244241ac6/MyPicture.jpg"
     */
    protected function getRelativePublicationPathAndFilename(ResourceMetaDataInterface $object): string
    {
        if ($object->getRelativePublicationPath() !== '') {
            $pathAndFilename = $object->getRelativePublicationPath() . $object->getFilename();
        } else {
            $pathAndFilename = $object->getSha1() . '/' . $object->getFilename();
        }
        return $pathAndFilename;
    }
}
