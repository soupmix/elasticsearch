# Soupmix


[![Latest Stable Version](https://poser.pugx.org/mkorkmaz/soupmix/v/stable)](https://packagist.org/packages/mkorkmaz/soupmix) [![Total Downloads](https://poser.pugx.org/mkorkmaz/soupmix/downloads)](https://packagist.org/packages/mkorkmaz/soupmix) [![Latest Unstable Version](https://poser.pugx.org/mkorkmaz/soupmix/v/unstable)](https://packagist.org/packages/mkorkmaz/soupmix) [![License](https://poser.pugx.org/mkorkmaz/soupmix/license)](https://packagist.org/packages/mkorkmaz/soupmix)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/mkorkmaz/soupmix/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/mkorkmaz/soupmix/)

Simple PHP low level database adapters collection to handle CRUD operations. This library does not provide any ORM or ODM. 

## Adapters

* **MongoDB: Exists**
* **Elasticsearch: Exists**
* Couchbase: Planned
* MySQL: Planned
* PostgreSQL: Planned




## Installation

It's recommended that you use [Composer](https://getcomposer.org/) to install Soupmix.

```bash
$ composer require  --prefer-dist mkorkmaz/soupmix "*"
```

This will install Soupmix and all required dependencies. Soupmix requires PHP 5.4.0 or newer, mongodb extension: 1.1.0 or newer, [mongo-php-library](https://github.com/mongodb/mongo-php-library) library  or newer form MongoDB, [elasticsearch-php](https://github.com/elastic/elasticsearch-php) library  or newer for Elasticsearch

## Documentation

[API Documentation](https://github.com/mkorkmaz/soupmix/blob/master/docs/API_Documentation.md): See details about the db adapters functions:

## Usage
```
// Connect to MongoDB Service
$adapter_config = [];
$adapter_config['db_name'] ='db_name';
$adapter_config['connection_string']="mongodb://127.0.0.1";
$adapter_config['options'] =[];
$m=new Soupmix\Adapters\MongoDB($adapter_config);

// Connect to Elasticsearch Service
$adapter_config 			= [];
$adapter_config['db_name'] 	= 'indexname';
$adapter_config['hosts']	= ["127.0.0.1:9200"];
$adapter_config['options'] 	= [];

$e=new Soupmix\Adapters\ElasticSearch($adapter_config);

$docs = [];
$docs[] = [
	"full_name" => "John Doe",
      "age" => 33,
      "email"	=> "johndoe@domain.com",
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
      "email"	=> "jackdoe@domain.com",
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
      "email"	=> "janedoe@domain.com",
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
	$mongo_user_id = $m->insert("users",$doc);
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
$filter=[['age__lte'=>34,'age__gte'=>36],"age__not"=>38];

//find users that has criteria encoded in $filter
$docs = $e->find("users", $filter);


```







## Contribute
* Open issue if found bugs or sent pull request.
* Feel free to ask if you have any questions.
