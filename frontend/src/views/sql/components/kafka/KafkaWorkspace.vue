<template>
  <div class="kafka-workspace">
    <KafkaGroupPanel v-if="isGroupMode" :tab="tab" :execute-query="executeQuery" :create-session="createSession" />
    <KafkaTopicPanel v-else :tab="tab" :execute-query="executeQuery" :create-session="createSession" />
  </div>
</template>

<script>
import KafkaTopicPanel from '@/views/sql/components/kafka/KafkaTopicPanel.vue';
import KafkaGroupPanel from '@/views/sql/components/kafka/KafkaGroupPanel.vue';

export default {
  name: 'KafkaWorkspace',
  components: {
    KafkaTopicPanel,
    KafkaGroupPanel
  },
  props: {
    tab: {
      type: Object,
      required: true
    },
    executeQuery: {
      type: Function,
      required: true
    },
    createSession: {
      type: Function,
      required: true
    }
  },
  computed: {
    schemaName() {
      return this.tab.node?.SCHEMA?.id || this.tab.node?.SCHEMA?.name || this.tab.selectValue || '';
    },
    isGroupMode() {
      const schema = String(this.schemaName).toLowerCase();
      return schema === 'groups' || schema === 'consumer groups' || schema === '消费组';
    }
  }
};
</script>

<style scoped lang="less">
.kafka-workspace {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--sql-workspace-panel-bg, #fff);
}
</style>
