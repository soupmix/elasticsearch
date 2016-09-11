<?php

namespace Soupmix;
/*
Elasticsearch Adapter
*/
Use Elasticsearch\Client;

final class ElasticSearch implements Base
{
    protected $conn = null;
    protected $index = null;
    protected $esVersion = 1;

    private static $operators = [
        'range'     => ['gt', 'gte', 'lt', 'lte'],
        'standart'  => ['prefix', 'regexp', 'wildchard'],
        'BooleanQueryLevel' => ['not'],
        'special'   => ['in']
    ];

    public function __construct($config, Client $client)
    {
        $this->index = $config['db_name'];
        $this->conn = $client;
        $this->esVersion = $client->info()['version']['number'];
    }


    public function getConnection()
    {
        return $this->conn;
    }

    public function create($collection, $fields)
    {
    }
    public function drop($collection)
    {
        $params = ['index' => $this->index];
        try {
            $this->conn->indices()->delete($params);
        } catch (\Exception $e) {
            // This ignore the error
            return false;
        }
        return true;
    }

    public function truncate($collection)
    {
    }

    public function createIndexes($collection, $indexes)
    {
    }

    public function insert($collection, $values)
    {
        $params = [];
        $params['body'] = $values;
        $params['index'] = $this->index;
        $params['type'] = $collection;
        try {
            $result = $this->conn->index($params);
            if ($result['created']) {
                return $result['_id'];
            }
            return null;
        } catch (\Exception $e) {
            return;
        }
    }

    public function get($collection, $docId)
    {
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;

        try {
            if (gettype($docId) == "array") {
                $params['body'] = [
                    'query' => [
                        'filtered' => [
                            'filter' => [
                                'ids' => ['values'=>$docId]
                            ],
                        ],
                    ],
                ];
                $results = $this->conn->search($params);
                if ($results['hits']['total'] == 0) {
                    return;
                }
                $result = [];
                foreach ($results['hits']['hits'] as $item){
                    $result[$item['_id']]=$item['_source'];
                }
                return $result;
            } else {
                $params['id'] = $docId;
                $result = $this->conn->get($params);
            }
            if ($result['found']) {
                $result['_source']['_id'] = $result['_id'];
                return $result['_source'];
            } else {
                return;
            }
        } catch (\Exception $e) {
            return;
        }
    }

    public function update($collection, $filter, $values)
    {
        $docs = $this->find($collection, $filter, ['_id']);
        if ($docs['total']===0) {
            return 0;
        }
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        $modified_count = 0;
        foreach ($docs['data'] as $doc) {
            $params['id'] = $doc['_id'];
            $params['body']['doc'] = $values;
            try {
                $this->conn->update($params);
                ++$modified_count;
            } catch (\Exception $e) {
                // throw new \Exception($e->getMessage());
            }
        }

        return $modified_count;
    }

    public function delete($collection, $filter)
    {
        if (isset($filter['_id'])) {
            $params = [];
            $params['index'] = $this->index;
            $params['type'] = $collection;
            $params['id'] = $filter['_id'];
            try {
                $result = $this->conn->delete($params);
                if ($result['found']) {
                    return 1;
                }
                return 0;
            } catch (\Exception $e) {
                return 0;
            }
        }
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        $params['fields'] = '_id';
        $result = $this->find($collection, $filter, ['_id'], null, 0, 1);
        if ($result['total']==1) {
            $params = [];
            $params['index'] = $this->index;
            $params['type'] = $collection;
            $params['id'] = $result['data']['_id'];
            try {
                $result = $this->conn->delete($params);
                if ($result['found']) {
                    return 1;
                }
                return 0;
            } catch (\Exception $e) {
                return 0;
            }
        }
        return 0;
    }

    public function find($collection, $filters, $fields = null, $sort = null, $start = 0, $limit = 25)
    {
        $return_type = '_source';
        $params = [];
        $params['index'] = $this->index;
        $params['type'] = $collection;
        if ($filters!==null) {
            $filters = self::buildFilter($filters);
            $params['body'] = [
                'query' => [
                    'filtered' => [
                        'filter' => [
                            'bool' => $filters,
                        ],
                    ],
                ],
            ];
        }
        $count = $this->conn->count($params);
        if ($fields!==null) {
            $params['fields'] = implode(',', $fields);
            $return_type = 'fields';
        }
        if ($sort!==null) {
            $params['sort'] = '';
            foreach ($sort as $sort_key => $sort_dir) {
                if ($params['sort']!='') {
                    $params['sort'] .= ',';
                }
                $params['sort'] .= $sort_key.':'.$sort_dir;
            }
        }
        if ($fields != null) {
            $params['fields'] = $fields;
            $return_type = 'fields';
        }
        $params['from'] = (int) $start;
        $params['size'] = (int) $limit;
        $return = $this->conn->search($params);
        if ($return['hits']['total']==0) {
            return ['total' => 0, 'data' => null];
        }
        elseif ($limit==1) {
            $return['hits']['hits'][0][$return_type]['_id'] = $return['hits']['hits'][0]['_id'];
            return ['total' => 1, 'data' => $return['hits']['hits'][0][$return_type]];
        }
        $result = [];
        foreach ($return['hits']['hits'] as $item) {
            if (($return_type == 'fields') && ($fields != ['_id'])) {
                foreach ($item[$return_type]as $k => $v) {
                    $item[$return_type][$k] = $v[0];
                }
            }
            $item[$return_type]['_id'] = $item['_id'];
            $result[] = $item[$return_type];
        }
        return ['total' => $count['count'], 'data' => $result];
    }

    public function query($collection)
    {
        return new ElasticSearchQueryBuilder($collection);
    }

    public static function buildFilter($filter)
    {
        $filters = [];
        foreach ($filter as $key => $value) {
            $isNot = '';
            if (strpos($key, '__')!==false) {
                $tmpFilters = self::buildFilterForKeys($key, $value, $isNot);
                $filters = self::mergeFilters($filters, $tmpFilters);
            } elseif ((strpos($key, '__') === false) && (is_array($value))) {
                $filters['should'] = self::buildFilterForOR($value);
            } else {
                $filters['must'][] = ['term' => [$key => $value]];
            }
        }
        return $filters;
    }

    private static function mergeFilters ($filters, $tmpFilters){
        foreach ($tmpFilters as $fKey => $fVals) {
            if (isset($filters[$fKey])) {
                foreach ($fVals as $fVal) {
                    $filters[$fKey][] = $fVal;
                }
            } else {
                $filters[$fKey] = $fVals;
            }
        }
        return $filters;
    }

    private static function buildFilterForKeys($key, $value, $isNot)
    {
        $filters = [];
        preg_match('/__(.*?)$/i', $key, $matches);
        $operator = $matches[1];
        if (strpos($operator, '!')===0) {
            $operator = str_replace('!', '', $operator);
            $isNot = '_not';
        }
        $key = str_replace($matches[0], '', $key);
        foreach (self::$operators as $type => $possibilities) {
            if (in_array($operator, $possibilities)) {
                switch ($type) {
                    case 'range':
                        $filters['must'.$isNot][] = ['range' => [$key => [$operator => $value]]];
                        break;
                    case 'standart':
                        $filters['must'.$isNot][] = [$type => [$key => $value]];
                        break;
                    case 'BooleanQueryLevel':
                        switch ($operator) {
                            case 'not':
                                $filters['must_not'][] = ['term' => [$key => $value]];
                                break;
                        }
                        break;
                    case 'special':
                        switch ($operator) {
                            case 'in':
                                $filters['must'.$isNot][] = ['terms' => [$key => $value]];
                                break;
                        }
                        break;
                }
            }
        }
        return $filters;
    }

    private static function buildFilterForOR($orValues)
    {
        $filters = [];
        foreach ($orValues as $orValue) {
            $subKey = array_keys($orValue)[0];
            $subValue = $orValue[$subKey];
            if (strpos($subKey, '__') !== false) {
                preg_match('/__(.*?)$/i', $subKey, $subMatches);
                $subOperator = $subMatches[1];
                if (strpos($subOperator, '!')===0) {
                    $subOperator = str_replace('!', '', $subOperator);
                }
                $subKey = str_replace($subMatches[0], '', $subKey);
                foreach (self::$operators as $type => $possibilities) {
                    if (in_array($subOperator, $possibilities)) {
                        switch ($type) {
                            case 'range':
                                $filters[] = ['range' => [$subKey => [$subOperator => $subValue]]];
                                break;
                            case 'standart':
                                $filters[] = [$type => [$subKey => $subValue]];
                                break;
                            case 'special':
                                switch ($subOperator) {
                                    case 'in':
                                        $filters[] = ['terms' => [$subKey => $subValue]];
                                        break;
                                }
                                break;
                        }
                    }
                }
            } else {
                $filters[] = ['term' => [$subKey => $subValue]];
            }
        }
        return $filters;
    }
}
