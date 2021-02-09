<?php declare(strict_types=1);

namespace ElasticMigrations\Adapters;

use ElasticAdapter\Indices\Index;
use ElasticAdapter\Indices\IndexManager;
use ElasticAdapter\Indices\Mapping;
use ElasticAdapter\Indices\Settings;
use ElasticMigrations\IndexManagerInterface;

class IndexManagerAdapter implements IndexManagerInterface
{
    /**
     * @var IndexManager
     */
    private $indexManager;

    public function __construct(IndexManager $indexManager)
    {
        $this->indexManager = $indexManager;
    }

    public function create(string $indexName, ?callable $modifier = null): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        if (isset($modifier)) {
            $mapping = new Mapping();
            $settings = new Settings();

            $modifier($mapping, $settings);

            $index = new Index($prefixedIndexName, $mapping, $settings);
        } else {
            $index = new Index($prefixedIndexName);
        }

        $this->indexManager->create($index);

        return $this;
    }

    public function createIfNotExists(string $indexName, ?callable $modifier = null): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        if (!$this->indexManager->exists($prefixedIndexName)) {
            $this->create($indexName, $modifier);
        }

        return $this;
    }

    public function putMapping(string $indexName, callable $modifier): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        $mapping = new Mapping();
        $modifier($mapping);
        $this->indexManager->putMapping($prefixedIndexName, $mapping);

        return $this;
    }

    public function putSettings(string $indexName, callable $modifier): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        $settings = new Settings();
        $modifier($settings);
        $this->indexManager->putSettings($prefixedIndexName, $settings);

        return $this;
    }

    public function putSettingsHard(string $indexName, callable $modifier): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        $this->indexManager->close($prefixedIndexName);
        $this->putSettings($indexName, $modifier);
        $this->indexManager->open($prefixedIndexName);

        return $this;
    }

    public function drop(string $indexName): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        $this->indexManager->drop($prefixedIndexName);

        return $this;
    }

    public function dropIfExists(string $indexName): IndexManagerInterface
    {
        $prefixedIndexName = $this->prefixIndexName($indexName);

        if ($this->indexManager->exists($prefixedIndexName)) {
            $this->drop($indexName);
        }

        return $this;
    }

    private function prefixIndexName(string $indexName): string
    {
        return config('elastic.migrations.index_name_prefix') . $indexName;
    }
}
