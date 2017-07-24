# Soupmix


[![Latest Stable Version](https://poser.pugx.org/soupmix/elasticsearch/v/stable)](https://packagist.org/packages/soupmix/elasticsearch) [![Total Downloads](https://poser.pugx.org/soupmix/elasticsearch/downloads)](https://packagist.org/packages/soupmix/elasticsearch) [![Latest Unstable Version](https://poser.pugx.org/soupmix/elasticsearch/v/unstable)](https://packagist.org/packages/soupmix/elasticsearch) [![License](https://poser.pugx.org/soupmix/elasticsearch/license)](https://packagist.org/packages/soupmix/elasticsearch)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/soupmix/elasticsearch/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/soupmix/elasticsearch/) [![Build Status](https://travis-ci.org/soupmix/elasticsearch.svg?branch=master)](https://travis-ci.org/soupmix/elasticsearch) [![Code Coverage](https://scrutinizer-ci.com/g/soupmix/elasticsearch/badges/coverage.png?b=master)](https://scrutinizer-ci.com/g/soupmix/elasticsearch/?branch=master)

Simple ElasticSearch abstraction layer adapter to handle CRUD operations written in PHP. This library does not provide any ORM or ODM. 


## Installation

It's recommended that you use [Composer](https://getcomposer.org/) to install Soupmix.

```bash
$ composer require soupmix/elasticsearch "~0.8"
```

This will install Soupmix and all required dependencies. Soupmix requires PHP 7.1 or newer, [elasticsearch-php](https://github.com/elastic/elasticsearch-php) library or newer for Elasticsearch

## Documentation

[API Documentation](https://github.com/soupmix/base/blob/master/docs/API_Documentation.md): See details about the db adapters functions:

## Usage
```

// Connect to Elasticsearch Service
$adapter_config             = [];
$adapter_config['db_name']  = 'indexname';
$adapter_config['hosts']    = ["127.0.0.1:9200"];
$adapter_config['options']  = [];
$config['db_name'] = $adapter_config['db_name];
$client = \Elasticsearch\ClientBuilder::create()->setHosts($adapter_config['hosts'])->build();

$e=new Soupmix\ElasticSearch($config, $client);

$docs = [];
$docs[] = [
    "full_name" => "John Doe",
      "age" => 33,
      "email"    => "johndoe@domain.com",
      "siblings"=> [
        "male"=> [
          "count"=> 1,
          "names"=> ["Jack"]
        ],
        "female"=> [
          "count" => 1,
          "names" =>["Jane"]
        ]      
      ]
];
$docs[] = [
    "full_name" => "Jack Doe",
      "age" => 38,
      "email"    => "jackdoe@domain.com",
      "siblings"=> [
        "male"=> [
          "count"=> 1,
          "names"=> ["John"]
        ],
        "female"=> [
          "count" => 1,
          "names" =>["Jane"]
        ]      
      ]
];

$docs[] = [
    "full_name" => "Jane Doe",
      "age" => 29,
      "email"    => "janedoe@domain.com",
      "siblings"=> [
        "male"=> [
          "count"=> 2,
          "names"=> ["Jack","John"]
        ],
        "female"=> [
          "count" => 0,
          "names" =>[]
        ]      
      ]
];

foreach($docs as $doc){
    // insert user into database
    $es_user_id = $e->insert("users",$doc);

}
// get user data using id
$es_user_data = $e->get('users', "AVPHZO1DY8UxeHDGBhPT");


$filter = ['age_gte'=>0];
// update users' data that has criteria encoded in $filter
$set = ['is_active'=>1,'is_deleted'=>0];

$e->update("users",$)

$filter = ["siblings.male.count__gte"=>2];

//delete users that has criteria encoded in $filter
$e->delete('users', $filter);



// user's age lower_than_and_equal to 34 or greater_than_and_equal 36 but not 38
$filter=[[['age__lte'=>34],['age__gte'=>36]],"age__not"=>38];

//find users that has criteria encoded in $filter
$docs = $e->find("users", $filter);


```



## Contribute
* Open issue if found bugs or sent pull request.
* Feel free to ask if you have any questions.
