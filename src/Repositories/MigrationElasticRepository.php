<?php declare(strict_types=1);

namespace ElasticMigrations\Repositories;

use ElasticMigrations\ReadinessInterface;
use Elasticsearch\Client;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use stdClass;

final class MigrationElasticRepository implements ReadinessInterface
{
    const MAX_SEARCH_COUNT = 10000;
    /**
     * @var string
     */
    private $index;
    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->index = config('elastic.migrations.migrations_index');
        $this->client = $client;
    }

    public function insert(string $fileName, int $batch): bool
    {
        $res = $this->client->index([
            'index' => $this->index,
            'refresh' => true,
            'body' => [
                'migration' => $fileName,
                'batch' => $batch,
            ],
        ]);
        return !empty($res['_id']);
    }

    public function exists(string $fileName): bool
    {
        $result = $this->client->search([
            'index' => $this->index,
            'body' => $this->getBodyQuery($fileName),
        ]);
        return !empty($result['hits']['hits']);
    }

    public function delete(string $fileName): bool
    {
        $result = $this->client->deleteByQuery([
            'index' => $this->index,
            'refresh' => true,
            'body' => $this->getBodyQuery($fileName),
        ]);
        return $result['deleted'] === 1;
    }

    public function getLastBatchNumber(): ?int
    {
        $result = $this->client->search([
            'index' => $this->index,
            'body' => [
                'size' => 1,
                'sort' => [
                    [
                        'batch' => [
                            'order' => 'desc',
                        ],
                    ],
                ],
            ],
        ]);
        return $result['hits']['hits'][0]['_source']['batch'] ?? null;
    }

    public function getLastBatch(): Collection
    {
        $result = $this->client->search([
            'index' => $this->index,
            'body' => [
                'size' => self::MAX_SEARCH_COUNT,
                'query' => [
                    'term' => [
                        'batch' => $this->getLastBatchNumber() ?: 0,
                    ],
                ],
                'sort' => [
                    [
                        'migration' => [
                            'order' => 'desc',
                        ],
                    ],
                ],
            ],
        ]);
        return collect(
            array_map(
                static function(array $item): string {
                    return $item['_source']['migration'];
                },
                $result['hits']['hits']
            )
        );
    }

    public function getAll(): Collection
    {
        $result = $this->client->search([
            'index' => $this->index,
            'body' => [
                'size' => self::MAX_SEARCH_COUNT,
                'query' => [
                    'match_all' => new stdClass(),
                ],
                'sort' => [
                    [
                        'migration' => [
                            'order' => 'desc',
                        ],
                    ],
                ],
            ],
        ]);
        return collect(
            array_map(
                static function(array $item): string {
                    return $item['_source']['migration'];
                },
                $result['hits']['hits']
            )
        );
    }


    public function isReady(): bool
    {
        return $this->client->indices()->exists(['index' => $this->index]);
    }

    private function getBodyQuery(string $name): array
    {
        return [
            'query' => [
                'term' => [
                    'migration' => $name,
                ],
            ],
        ];
    }
}
