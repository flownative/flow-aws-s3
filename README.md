[![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)
![Packagist][packagist]

[packagist]: https://img.shields.io/packagist/v/flownative/aws-s3.svg

# AWS S3 Adaptor for Neos 2.x and Flow 3.x

This [Flow](https://flow.typo3.org) package allows you to store assets (resources) in [Amazon's S3](https://aws.amazon.com/s3/) and publish resources to S3 or [Cloudfront](https://aws.amazon.com/cloudfront/). Because [Neos CMS](https://www.neos.io) is using Flow's resource management under the hood, this adaptor also works nicely for all kinds of assets in Neos.

## Key Features

- store all assets or only a specific collection in a private S3 bucket
- publish assets to a private or public S3 bucket
- command line interface for basic tasks like connection check or emptying an S3 bucket

Using this connector, you can run a Neos website which does not store any asset (images, PDFs etc.) on your local webserver.

## Installation

The Flownative AWS S3 connector is installed as a regular Flow package via Composer. For your existing project, simply
include `flownative/aws-s3` into the dependencies of your Flow or Neos distribution:

```bash
    $ composer require flownative/aws-s3:~1.0@beta
```

## Configuration

In order to communicate with the AWS web service, you need to provide the credentials of an account which has access
to S3 (see next section for instructions for setting up the user in AWS IAM). Add the following configuration to the
`Settings.yaml` for your desired Flow context (for example in `Configuration/Production/Settings.yaml`) and make sure
to replace key, secret and region with your own data:
  
```yaml
Flownative:
  Aws:
    S3:
      profiles:
        default:
          credentials:
            key: 'CD2ADVB134LQ9SFICAJB'
            secret: 'ak1KJAnotasecret9JamNkwYY188872MyljWJ'
          region: 'eu-central-1'
```

You can test your settings by executing the `connect` command:

```bash
    $ ./flow s3:connect
    OK
```

## IAM Setup

tbd.

## Publish Assets to S3 / Cloufront

Once the connector package is in place, you add a new publishing target which uses that connect and assign this target
to your collection.

```yaml

  TYPO3:
    Flow:
      resource:
        collections:
          persistent:
            target: 'cloudFrontPersistentResourcesTarget'
        targets:
          cloudFrontPersistentResourcesTarget:
            target: 'Flownative\Aws\S3\S3Target'
            targetOptions:
              bucket: 'media.example.com'
              keyPrefix: '/'
              baseUri: 'https://abc123def456.cloudfront.net/'
```

Since the new publishing target will be empty initially, you need to publish your assets to the new target by using the  ``resource:publish`` command:

```bash
    path$ ./flow resource:publish
```

This command will upload your files to the target and use the calculated remote URL for all your assets from now on.

## Switching the Storage of a Collection

If you want to migrate from your default local filesystem storage to a remote storage, you need to copy all your existing persistent resources to that new storage and use that storage afterwards by default.

You start by adding a new storage with the S3 connector to your configuration. As you might want also want to serve your assets by the remote storage system, you also add a target that contains your published resources.

```yaml

  TYPO3:
    Flow:
      resource:
        storages:
          s3PersistentResourcesStorage:
            storage: 'Flownative\Aws\S3\S3Storage'
            storageOptions:
              bucket: 'storage.neos.example.com'
              keyPrefix: 'mywebsite.com/'
        targets:
          s3PersistentResourcesTarget:
            target: 'Flownative\Aws\S3\S3Target'
            targetOptions:
              bucket: 'media.neos.example.com'
              keyPrefix: 'mywebsite.com/'
              baseUri: 'https://abc123def456.cloudfront.net/'
```

Some notes regarding the configuration:

You must create separate buckets for storage and target respectively, because the storage will remain private and the target will potentially be published. Even if it might work using one bucket for both, this is a non-supported setup.

The `keyPrefix` option allows you to share one bucket accross multiple websites or applications. All S3 objects keys will be prefiexd by the given string.

The `baseUri` option defines the root of the publicly accessible address pointing to your published resources. In the example above, baseUri points to a Cloudfront subdomain which needs to be set up separately. It is rarely a good idea to the public URI of S3 objects directly (like, for example "https://s3.eu-central-1.amazonaws.com/target.neos.example.com/mywebsite.com/00889c4636cd77876e154796d469955e567ce23c/NeosCMS-2507x3347.jpg") because S3 is usually too slow for being used as a server for common assets on your website. It's good for downloads, but not for your CSS files or photos.

In order to copy the resources to the new storage we need a temporary collection that uses the storage and the new publication target.

```yaml

  TYPO3:
    Flow:
      resource:
        collections:
          tmpNewCollection:
            storage: 's3PersistentResourcesStorage'
            target: 's3PersistentResourcesTarget'
```

Now you can use the ``resource:copy`` command (available in Flow 3.1 or Neos 2.1 and higher):

```bash

    $ ./flow resource:copy --publish persistent tmpNewCollection

```

This will copy all your files from your current storage (local filesystem) to the new remote storage. The ``--publish`` flag means that this command also publishes all the resources to the new target, and you have the same state on your current storage and publication target as on the new one.

Now you can overwrite your old collection configuration and remove the temporary one:

```yaml

  TYPO3:
    Flow:
      resource:
        collections:
          persistent:
            storage: 's3PersistentResourcesStorage'
            target: 's3PersistentResourcesTarget'
```

Clear caches and you're done.

```bash

    $ ./flow flow:cache:flush

```

## Full Example Configuration

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
          credentials:
            key: 'CD2ADVB134LQ9SFICAJB'
            secret: 'ak1KJAnotasecret9JamNkwYY188872MyljWJ'
          region: 'eu-central-1'
```
