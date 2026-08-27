<template>
  <div class="kafka-topic-panel">
    <div class="kafka-topic-panel__header">
      <div class="kafka-topic-panel__connection">
        <a-select
          v-if="tab.selectOptions"
          class="schema-select-style"
          v-model:value="tab.selectValue"
          show-search
          size="small"
          :options="tab.selectOptions || []"
          disabled
        />
        <CustomIcon
          class="query-connection-icon"
          :type="tab.dsType"
          :instance-type="tab.node.INSTANCE.attr.dsDeployType"
          size="14px"
          aria-hidden="true"
        />
        <div class="query-connection-label">@{{ tab.node.INSTANCE.attr.dsHost }}</div>
      </div>
      <div class="kafka-topic-panel__actions">
        <Button size="small" :loading="loading" @click="loadTopic" :disabled="!topicName">
          {{ $t('shua-xin') }}
        </Button>
        <Button size="small" type="primary" :loading="saving" :disabled="!topicName || !dirty" @click="handleSave">
          {{ $t('bao-cun') }}
        </Button>
      </div>
    </div>

    <div v-if="!topicName" class="kafka-topic-panel__empty">
      {{ $t('kafka-topic-select-hint') }}
    </div>

    <div v-else class="kafka-topic-panel__body">
      <a-spin :spinning="loading">
        <div class="kafka-topic-panel__section">
          <div class="kafka-topic-panel__title">{{ $t('kafka-topic-overview') }}</div>
          <div class="kafka-topic-panel__metrics">
            <div class="kafka-topic-panel__metric">
              <span class="kafka-topic-panel__metric-label">{{ $t('kafka-topic-partition-count') }}</span>
              <span class="kafka-topic-panel__metric-value">{{ topicInfo.partitionCount ?? '-' }}</span>
            </div>
            <div class="kafka-topic-panel__metric">
              <span class="kafka-topic-panel__metric-label">{{ $t('kafka-topic-replication-factor') }}</span>
              <span class="kafka-topic-panel__metric-value">{{ topicInfo.replicationFactor ?? '-' }}</span>
            </div>
            <div class="kafka-topic-panel__metric">
              <span class="kafka-topic-panel__metric-label">{{ $t('kafka-topic-min-isr') }}</span>
              <span class="kafka-topic-panel__metric-value">{{ topicInfo.minIsr ?? '-' }}</span>
            </div>
            <div class="kafka-topic-panel__metric">
              <span class="kafka-topic-panel__metric-label">{{ $t('kafka-topic-cleanup-policy') }}</span>
              <span class="kafka-topic-panel__metric-value">{{ topicInfo.cleanupPolicy || '-' }}</span>
            </div>
          </div>
        </div>

        <div class="kafka-topic-panel__section">
          <div class="kafka-topic-panel__title">{{ $t('kafka-topic-settings') }}</div>
          <div class="kafka-topic-panel__form">
            <div class="kafka-topic-panel__field">
              <div class="kafka-topic-panel__field-label">{{ $t('kafka-topic-retention-hours') }}</div>
              <InputNumber
                :model-value="form.retentionHours"
                :min="1"
                :precision="0"
                style="width: 100%"
                @on-change="(value) => updateFormField('retentionHours', value)"
              />
            </div>
            <div class="kafka-topic-panel__field">
              <div class="kafka-topic-panel__field-label">{{ $t('kafka-topic-min-isr') }}</div>
              <InputNumber
                :model-value="form.minIsr"
                :min="1"
                :max="topicInfo.replicationFactor || 99"
                :precision="0"
                style="width: 100%"
                @on-change="(value) => updateFormField('minIsr', value)"
              />
            </div>
            <div class="kafka-topic-panel__field">
              <div class="kafka-topic-panel__field-label">{{ $t('kafka-topic-partition-count') }}</div>
              <InputNumber
                :model-value="form.partitionCount"
                :min="topicInfo.partitionCount || 1"
                :precision="0"
                style="width: 100%"
                @on-change="(value) => updateFormField('partitionCount', value)"
              />
              <div class="kafka-topic-panel__hint">{{ $t('kafka-topic-partition-hint') }}</div>
            </div>
          </div>
        </div>

        <div class="kafka-topic-panel__section">
          <div class="kafka-topic-panel__title">{{ $t('kafka-topic-partition-detail') }}</div>
          <a-table
            size="small"
            bordered
            :pagination="false"
            :scroll="{ y: 280 }"
            :columns="partitionColumns"
            :data-source="partitionRows"
            row-key="PARTITION"
          />
        </div>
      </a-spin>
    </div>
  </div>
</template>

<script>
import CustomIcon from '@/components/function/CustomIcon.vue';

export default {
  name: 'KafkaTopicPanel',
  components: {
    CustomIcon
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
  data() {
    return {
      loading: false,
      saving: false,
      loadSeq: 0,
      topicInfo: {
        partitionCount: null,
        replicationFactor: null,
        minIsr: null,
        retentionMs: null,
        cleanupPolicy: ''
      },
      form: {
        retentionHours: null,
        minIsr: null,
        partitionCount: null
      },
      original: {
        retentionHours: null,
        minIsr: null,
        partitionCount: null
      },
      partitionRows: []
    };
  },
  computed: {
    topicName() {
      const table = this.tab.selectedTable;
      if (!table) {
        return '';
      }
      return table.objName || table.title || '';
    },
    dirty() {
      return (
        this.normalizeInt(this.form.retentionHours) !== this.normalizeInt(this.original.retentionHours) ||
        this.normalizeInt(this.form.minIsr) !== this.normalizeInt(this.original.minIsr) ||
        this.normalizeInt(this.form.partitionCount) !== this.normalizeInt(this.original.partitionCount)
      );
    },
    partitionColumns() {
      return [
        { title: this.$t('kafka-topic-partition-id'), dataIndex: 'PARTITION', key: 'PARTITION', width: 100 },
        { title: 'Leader', dataIndex: 'LEADER', key: 'LEADER', width: 100 },
        { title: this.$t('kafka-topic-replicas'), dataIndex: 'REPLICAS', key: 'REPLICAS' },
        { title: 'ISR', dataIndex: 'ISR', key: 'ISR' }
      ];
    }
  },
  watch: {
    topicName: {
      immediate: true,
      handler(value) {
        if (value) {
          this.$nextTick(() => {
            this.loadTopic();
          });
        } else {
          this.resetState();
        }
      }
    }
  },
  methods: {
    resetState() {
      this.loadSeq += 1;
      this.topicInfo = {
        partitionCount: null,
        replicationFactor: null,
        minIsr: null,
        retentionMs: null,
        cleanupPolicy: ''
      };
      this.partitionRows = [];
      this.form = { retentionHours: null, minIsr: null, partitionCount: null };
      this.original = { retentionHours: null, minIsr: null, partitionCount: null };
    },
    updateFormField(field, value) {
      this.form[field] = this.normalizeInt(value);
    },
    quoteTopic(topic) {
      // Always quote so keywords (INFO/SET/...) and digit-leading names parse correctly.
      return `'${String(topic).replace(/'/g, "''")}'`;
    },
    normalizeInt(value) {
      if (value === null || value === undefined || value === '') {
        return null;
      }
      const number = Number(value);
      if (!Number.isFinite(number)) {
        return null;
      }
      return Math.trunc(number);
    },
    rowValue(row, key) {
      if (!row) {
        return null;
      }
      if (row[key] != null) {
        return row[key];
      }
      const matched = Object.keys(row).find((item) => String(item).toUpperCase() === key.toUpperCase());
      return matched == null ? null : row[matched];
    },
    msToHours(ms) {
      const value = Number(ms);
      if (!Number.isFinite(value) || value < 0) {
        return null;
      }
      return Math.round(value / 3600000);
    },
    hoursToMs(hours) {
      return this.normalizeInt(hours) * 3600000;
    },
    async ensureSession() {
      if (!this.tab.sessionId) {
        await this.createSession(this.tab);
      }
      if (!this.tab.sessionId) {
        throw new Error(this.$t('kafka-topic-load-failed'));
      }
    },
    applyTopicInfo(row) {
      const retentionHours = this.msToHours(this.rowValue(row, 'RETENTION_MS'));
      const partitionCount = this.normalizeInt(this.rowValue(row, 'PARTITION_COUNT'));
      const replicationFactor = this.normalizeInt(this.rowValue(row, 'REPLICATION_FACTOR'));
      const minIsr = this.normalizeInt(this.rowValue(row, 'MIN_ISR'));
      const retentionMs = Number(this.rowValue(row, 'RETENTION_MS'));
      this.topicInfo = {
        partitionCount,
        replicationFactor,
        minIsr,
        retentionMs: Number.isFinite(retentionMs) ? retentionMs : null,
        cleanupPolicy: this.rowValue(row, 'CLEANUP_POLICY') || ''
      };
      this.form = {
        retentionHours,
        minIsr,
        partitionCount
      };
      this.original = { ...this.form };
    },
    async loadTopic() {
      if (!this.topicName) {
        return;
      }
      const seq = ++this.loadSeq;
      this.loading = true;
      try {
        await this.ensureSession();
        if (seq !== this.loadSeq) {
          return;
        }
        const topic = this.quoteTopic(this.topicName);
        const infoRows = await this.executeQuery(this.tab, `DESCRIBE TOPIC ${topic} INFO`);
        if (seq !== this.loadSeq) {
          return;
        }
        if (!infoRows.length) {
          throw new Error(this.$t('kafka-topic-load-failed'));
        }
        this.applyTopicInfo(infoRows[0]);
        const partitionRows = await this.executeQuery(this.tab, `DESCRIBE TOPIC ${topic}`);
        if (seq !== this.loadSeq) {
          return;
        }
        this.partitionRows = partitionRows;
      } catch (error) {
        if (seq !== this.loadSeq) {
          return;
        }
        this.topicInfo = {
          partitionCount: null,
          replicationFactor: null,
          minIsr: null,
          retentionMs: null,
          cleanupPolicy: ''
        };
        this.partitionRows = [];
        this.form = { retentionHours: null, minIsr: null, partitionCount: null };
        this.original = { retentionHours: null, minIsr: null, partitionCount: null };
        this.$Message.error(error.message || this.$t('kafka-topic-load-failed'));
      } finally {
        if (seq === this.loadSeq) {
          this.loading = false;
        }
      }
    },
    async handleSave() {
      if (!this.topicName || !this.dirty) {
        return;
      }
      const retentionHours = this.normalizeInt(this.form.retentionHours);
      const minIsr = this.normalizeInt(this.form.minIsr);
      const partitionCount = this.normalizeInt(this.form.partitionCount);
      const originalRetentionHours = this.normalizeInt(this.original.retentionHours);
      const originalMinIsr = this.normalizeInt(this.original.minIsr);
      const originalPartitionCount = this.normalizeInt(this.original.partitionCount);
      if (partitionCount != null && this.topicInfo.partitionCount != null && partitionCount < this.topicInfo.partitionCount) {
        this.$Message.warning(this.$t('kafka-topic-partition-hint'));
        return;
      }
      this.saving = true;
      try {
        await this.ensureSession();
        const topic = this.quoteTopic(this.topicName);
        const commands = [];
        if (retentionHours !== originalRetentionHours && retentionHours != null) {
          commands.push(`ALTER TOPIC ${topic} SET retention.ms = ${this.hoursToMs(retentionHours)}`);
        }
        if (minIsr !== originalMinIsr && minIsr != null) {
          commands.push(`ALTER TOPIC ${topic} SET min.insync.replicas = ${minIsr}`);
        }
        if (partitionCount !== originalPartitionCount && partitionCount != null) {
          commands.push(`ALTER TOPIC ${topic} ADD PARTITIONS ${partitionCount}`);
        }
        if (!commands.length) {
          return;
        }
        for (const command of commands) {
          await this.executeQuery(this.tab, command);
        }
        this.$Message.success(this.$t('kafka-topic-save-success'));
        await this.loadTopic();
      } catch (error) {
        this.$Message.error(error.message || this.$t('kafka-topic-save-failed'));
      } finally {
        this.saving = false;
      }
    }
  }
};
</script>

<style scoped lang="less">
.kafka-topic-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--sql-workspace-panel-bg, #fff);
}

.kafka-topic-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--border-color-base, #e8eaec);
}

.kafka-topic-panel__connection {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.kafka-topic-panel__actions {
  display: flex;
  gap: 8px;
}

.kafka-topic-panel__empty {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-color-secondary, #808695);
  padding: 24px;
}

.kafka-topic-panel__body {
  flex: 1;
  overflow: auto;
  padding: 16px;
}

.kafka-topic-panel__section + .kafka-topic-panel__section {
  margin-top: 20px;
}

.kafka-topic-panel__title {
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
}

.kafka-topic-panel__metrics {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
}

.kafka-topic-panel__metric {
  border: 1px solid var(--border-color-base, #e8eaec);
  border-radius: 6px;
  padding: 12px;
}

.kafka-topic-panel__metric-label {
  display: block;
  color: var(--text-color-secondary, #808695);
  font-size: 12px;
  margin-bottom: 6px;
}

.kafka-topic-panel__metric-value {
  font-size: 18px;
  font-weight: 600;
}

.kafka-topic-panel__form {
  max-width: 420px;
}

.kafka-topic-panel__field + .kafka-topic-panel__field {
  margin-top: 12px;
}

.kafka-topic-panel__field-label {
  margin-bottom: 6px;
  color: var(--text-color, #515a6e);
  font-size: 13px;
}

.kafka-topic-panel__hint {
  margin-top: 6px;
  color: var(--text-color-secondary, #808695);
  font-size: 12px;
}
</style>
