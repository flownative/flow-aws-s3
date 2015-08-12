# AWS S3 Adaptor for Neos 2.x and Flow 3.x #

This Flow package allows you to store assets (resources) in S3 and publish resources to S3 (or Cloudfront).

Example configuration:

```yaml
TYPO3:
  Flow:

    resource:
      storages:
        s3PersistentResourcesStorage:
          storage: 'Flownative\Aws\S3\S3Storage'
          storageOptions:
            bucket: 'storage.neos.prd.fra.flownative.net'
            keyPrefix: 'flownative/neosio/'

      collections:

      # Collection which contains all persistent resources
        persistent:
          storage: 's3PersistentResourcesStorage'
          target: 's3PersistentResourcesTarget'

      targets:
        localWebDirectoryPersistentResourcesTarget:
          target: 'TYPO3\Flow\Resource\Target\FileSystemTarget'
          targetOptions:
            path: '%FLOW_PATH_WEB%_Resources/Persistent/'
            baseUri: '_Resources/Persistent/'
            subdivideHashPathSegment: false

        s3PersistentResourcesTarget:
          target: 'Flownative\Aws\S3\S3Target'
          targetOptions:
            bucket: 'target.neos.prd.fra.flownative.net'
            keyPrefix: 'flownative/neosio/'
            baseUri: 'https://d1z3d9iccwfvx7.cloudfront.net/'

Flownative:
  Aws:
    S3:
      profiles:
        default:
          signature: 'v4'
          credentials:
            key: 'CD2ADVB134LQ9SFICAJB'
            secret: 'ak1KJAnotasecret9JamNkwYY188872MyljWJ'
          region: 'eu-central-1'
```
