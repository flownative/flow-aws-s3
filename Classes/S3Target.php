<?php
namespace Flownative\Aws\S3;

/*                                                                        *
 * This script belongs to the package "Flownative.Aws.S3".                *
 *                                                                        *
 *                                                                        */

use Aws\S3\Exception\S3Exception;
use Aws\S3\S3Client;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\ResourceManagement\CollectionInterface;
use Neos\Flow\ResourceManagement\Exception;
use Neos\Flow\ResourceManagement\Publishing\MessageCollector;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\ResourceMetaDataInterface;
use Neos\Flow\ResourceManagement\Target\TargetInterface;
use Psr\Log\LoggerInterface;

/**
 * A resource publishing target based on Amazon S3
 */
class S3Target implements TargetInterface
{
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
     * CORS (Cross-Origin Resource Sharing) allowed origins for published content
     *
     * @var string
     */
    protected $corsAllowOrigin = '*';

    /**
     * @var string
     */
    protected $baseUri;

    /**
     * if TRUE (default), resources which are not anymore part of the storage will be removed
     * from the target as well. If set to FALSE, your target will only ever grow, never shrink.
     *
     * @var boolean
     */
    protected $unpublishResources = true;

    /**
     * If `true` (default) the S3 ACL is set to `public-read`. If `false` no ACL option will be set.
     *
     * @var boolean
     */
    protected $accessPolicyEnabled = true;

    /**
     * Internal cache for known storages, indexed by storage name
     *
     * @var array<\Neos\Flow\ResourceManagement\Storage\StorageInterface>
     */
    protected $storages = array();

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
    protected $existingObjectsInfo;

    /**
     * Constructor
     *
     * @param string $name Name of this target instance, according to the resource settings
     * @param array $options Options for this target
     * @throws Exception
     */
    public function __construct($name, array $options = array())
    {
        $this->name = $name;
        foreach ($options as $key => $value) {
            switch ($key) {
                case 'bucket':
                    $this->bucketName = $value;
                    break;
                case 'keyPrefix':
                    $this->keyPrefix = $value;
                    break;
                case 'corsAllowOrigin':
                    $this->corsAllowOrigin = $value;
                    break;
                case 'baseUri':
                    $this->baseUri = $value;
                    break;
                case 'unpublishResources':
                    $this->unpublishResources = (bool)$value;
                    break;
                case 'accessPolicyEnabled':
                    $this->accessPolicyEnabled = (bool)$value;
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
    public function initializeObject()
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
    public function getName()
    {
        return $this->name;
    }

    /**
     * Returns the S3 object key prefix
     *
     * @return string
     */
    public function getKeyPrefix()
    {
        return $this->keyPrefix;
    }

    /**
     * Publishes the whole collection to this target
     *
     * @param \Neos\Flow\ResourceManagement\CollectionInterface $collection The collection to publish
     * @param callable $callback Function called after each resource publishing
     * @return void
     * @throws Exception
     */
    public function publishCollection(CollectionInterface $collection, callable $callback = null)
    {
        if (!isset($this->existingObjectsInfo)) {
            $this->existingObjectsInfo = array();
            $requestArguments = array(
                'Bucket' => $this->bucketName,
                'Prefix' => $this->keyPrefix
            );

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

        $storage = $collection->getStorage();
        if ($storage instanceof S3Storage) {
            $storageBucketName = $storage->getBucketName();
            if ($storageBucketName === $this->bucketName && $storage->getKeyPrefix() === $this->keyPrefix) {
                throw new Exception(sprintf('Could not publish collection %s because the source and target S3 bucket is the same, with identical key prefixes. Either choose a different bucket or at least key prefix for the target.', $collection->getName()), 1428929137);
            }
            foreach ($collection->getObjects($callback) as $object) {
                /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
                $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
                if (array_key_exists($objectName, $potentiallyObsoleteObjects)) {
                    $this->systemLogger->debug(sprintf('The resource object "%s" (MD5: %s) has already been published to bucket "%s", no need to re-publish', $objectName, $object->getMd5() ?: 'unknown', $this->bucketName));
                    unset($potentiallyObsoleteObjects[$objectName]);
                } else {
                    $options = array(
                        'Bucket' => $this->bucketName,
                        'CopySource' => urlencode($storageBucketName . '/' . $storage->getKeyPrefix() . $object->getSha1()),
                        'ContentType' => $object->getMediaType(),
                        'MetadataDirective' => 'REPLACE',
                        'Key' => $objectName
                    );
                    if ($this->accessPolicyEnabled !== false) {
                        $options['ACL'] = 'public-read';
                    }
                    try {
                        $this->s3Client->copyObject($options);
                        $this->systemLogger->debug(sprintf('Successfully copied resource as object "%s" (MD5: %s) from bucket "%s" to bucket "%s"', $objectName, $object->getMd5() ?: 'unknown', $storageBucketName, $this->bucketName));
                    } catch (S3Exception $e) {
                        $message = sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName, $e->getMessage());
                        $this->systemLogger->critical($e);
                        $this->messageCollector->append($message);
                    }
                }
            }
        } else {
            foreach ($collection->getObjects() as $object) {
                /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
                $this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
                $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
                unset($potentiallyObsoleteObjects[$objectName]);
            }
        }

        if ($this->unpublishResources !== false) {
            foreach (array_keys($potentiallyObsoleteObjects) as $relativePathAndFilename) {
                $this->systemLogger->debug(sprintf('Deleted obsolete resource "%s" from bucket "%s"', $relativePathAndFilename, $this->bucketName));
                $this->s3Client->deleteObject(array(
                    'Bucket' => $this->bucketName,
                    'Key' => $this->keyPrefix . $relativePathAndFilename
                ));
            }
        } else {
            $this->systemLogger->debug(sprintf('Skipping resource unpublishing from bucket "%s", because configuration option "unpublishResources" is FALSE.', $this->bucketName));
        }
    }

    /**
     * Returns the web accessible URI pointing to the given static resource
     *
     * @param string $relativePathAndFilename Relative path and filename of the static resource
     * @return string The URI
     */
    public function getPublicStaticResourceUri($relativePathAndFilename)
    {
        if ($this->baseUri != '') {
            return $this->baseUri . $relativePathAndFilename;
        } else {
            return $this->s3Client->getObjectUrl($this->bucketName, $this->keyPrefix . $relativePathAndFilename);
        }
    }

    /**
     * Publishes the given persistent resource from the given storage
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource The resource to publish
     * @param CollectionInterface $collection The collection the given resource belongs to
     * @return void
     * @throws Exception
     */
    public function publishResource(PersistentResource $resource, CollectionInterface $collection)
    {
        $storage = $collection->getStorage();
        if ($storage instanceof S3Storage) {
            if ($storage->getBucketName() === $this->bucketName && $storage->getKeyPrefix() === $this->keyPrefix) {
                throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s because the source and target S3 bucket is the same, with identical key prefixes. Either choose a different bucket or at least key prefix for the target.', $resource->getSha1(), $collection->getName()), 1428929563);
            }
            try {
                $sourceObjectArn = $storage->getBucketName() . '/' . $storage->getKeyPrefix() . $resource->getSha1();
                $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
                $options = array(
                    'Bucket' => $this->bucketName,
                    'CopySource' => urlencode($sourceObjectArn),
                    'ContentType'=> $resource->getMediaType(),
                    'MetadataDirective' => 'REPLACE',
                    'Key' => $objectName
                );
                if ($this->accessPolicyEnabled !== false) {
                    $options['ACL'] = 'public-read';
                }
                $this->s3Client->copyObject($options);
                $this->systemLogger->debug(sprintf('Successfully published resource as object "%s" (MD5: %s) by copying from bucket "%s" to bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown', $storage->getBucketName(), $this->bucketName));
            } catch (S3Exception $e) {
                $message = sprintf('Could not publish resource with SHA1 hash %s of collection %s (source object: %s) through "CopyObject" because the S3 client reported an error: %s', $resource->getSha1(), $collection->getName(), $sourceObjectArn, $e->getMessage());
                $this->systemLogger->critical($e);
                $this->messageCollector->append($message);
            }
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

    /**
     * Unpublishes the given persistent resource
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource The resource to unpublish
     * @return void
     */
    public function unpublishResource(PersistentResource $resource)
    {
        if ($this->unpublishResources === false) {
            $this->systemLogger->debug(sprintf('Skipping resource unpublishing %s from bucket "%s", because configuration option "unpublishResources" is FALSE.', $resource->getMd5() ?: 'unknown', $this->bucketName));
            return;
        }

        try {
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
            $this->s3Client->deleteObject(array(
                'Bucket' => $this->bucketName,
                'Key' => $objectName
            ));
            $this->systemLogger->debug(sprintf('Successfully unpublished resource as object "%s" (MD5: %s) from bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown', $this->bucketName));
        } catch (\Exception $e) {
        }
    }

    /**
     * Returns the web accessible URI pointing to the specified persistent resource
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource Resource object or the resource hash of the resource
     * @return string The URI
     * @throws Exception
     */
    public function getPublicPersistentResourceUri(PersistentResource $resource)
    {
        if ($this->baseUri != '') {
            return $this->baseUri . $this->encodeRelativePathAndFilenameForUri($this->getRelativePublicationPathAndFilename($resource));
        } else {
            return $this->s3Client->getObjectUrl($this->bucketName, $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource));
        }
    }

	/**
	 * Applies rawurlencode() to all path segments of the given $relativePathAndFilename
	 *
	 * @param string $relativePathAndFilename
	 * @return string
	 */
	protected function encodeRelativePathAndFilenameForUri($relativePathAndFilename)
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
    protected function publishFile($sourceStream, $relativeTargetPathAndFilename, ResourceMetaDataInterface $metaData)
    {
        $objectName = $this->keyPrefix . $relativeTargetPathAndFilename;
        $options = array(
            'params' => array(
                'ContentLength' => $metaData->getFileSize(),
                'ContentType' => $metaData->getMediaType()
            )
        );

        try {
            $this->s3Client->upload($this->bucketName, $objectName, $sourceStream, $this->accessPolicyEnabled !== false ? 'public-read' : null, $options);
            $this->systemLogger->debug(sprintf('Successfully published resource as object "%s" in bucket "%s" with MD5 hash "%s"', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown'));
        } catch (\Exception $e) {
            $this->systemLogger->debug(sprintf('Failed publishing resource as object "%s" in bucket "%s" with MD5 hash "%s": %s', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown', $e->getMessage()));
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
    protected function getRelativePublicationPathAndFilename(ResourceMetaDataInterface $object)
    {
        if ($object->getRelativePublicationPath() !== '') {
            $pathAndFilename = $object->getRelativePublicationPath() . $object->getFilename();
        } else {
            $pathAndFilename = $object->getSha1() . '/' . $object->getFilename();
        }
        return $pathAndFilename;
    }
}
