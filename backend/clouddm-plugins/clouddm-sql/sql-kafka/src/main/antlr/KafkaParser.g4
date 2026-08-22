/*
 * Copyright 2026 杭州开云集致科技有限公司
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

parser grammar KafkaParser;
options { tokenVocab = KafkaLexer; }

rootInstSet : commands? EOF;

commands : cmdInst (SEMI cmdInst)* SEMI?;

cmdInst : command;

command:
    SHOW TOPICS #cmdShowTopics
    | SHOW GROUPS #cmdShowGroups
    | DESCRIBE CLUSTER #cmdDescribeCluster
    | DESCRIBE TOPIC topicName INFO #cmdDescribeTopicInfo
    | DESCRIBE TOPIC topicName #cmdDescribeTopic
    | DESCRIBE GROUP groupName (TOPIC topicName)? #cmdDescribeGroup
    | DELETE GROUP groupName #cmdDeleteGroup
    | ALTER TOPIC topicName SET configKey EQ configValue #cmdAlterTopicSet
    | ALTER TOPIC topicName ADD PARTITIONS intValue #cmdAlterTopicAddPartitions
    | ALTER GROUP groupName RESET OFFSET TO resetTarget resetScope* #cmdAlterGroupResetOffset
    | CONSUME topicName consumeOpt* #cmdConsume
    ;

topicName   : IDENT | STRING | INTEGER | keywordName;
groupName   : IDENT | STRING | INTEGER | keywordName;
configKey   : IDENT | STRING | keywordName;
configValue : IDENT | STRING | INTEGER | ARG | keywordName;
intValue    : INTEGER | ARG;
longValue   : INTEGER | ARG;
keywordName : SHOW | DESCRIBE | ALTER | DELETE | CONSUME | TOPICS | GROUPS | GROUP | TOPIC | CLUSTER
            | INFO | SET | ADD | RESET | OFFSET | TO | TIMESTAMP | PARTITIONS | FROM | BEGINNING | LATEST
            | PARTITION | LIMIT
            ;

resetTarget:
    BEGINNING
    | LATEST
    | TIMESTAMP longValue
    | longValue
    ;

resetScope:
    TOPIC topicName
    | PARTITION intValue
    ;

consumeOpt:
    FROM (BEGINNING | LATEST)
    | PARTITION intValue
    | LIMIT intValue
    ;
