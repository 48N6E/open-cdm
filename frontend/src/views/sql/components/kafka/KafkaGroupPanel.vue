<template>
  <div class="kafka-group-panel">
    <div class="kafka-group-panel__header">
      <div class="kafka-group-panel__connection">
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
      <div class="kafka-group-panel__actions">
        <Button size="small" :loading="loading" :disabled="!groupId" @click="loadGroupDetail">{{ $t('shua-xin') }}</Button>
        <Button size="small" type="primary" :disabled="!groupId" @click="openResetModal">
          {{ $t('kafka-group-reset-offset') }}
        </Button>
        <Button size="small" type="error" :disabled="!groupId" :loading="deleting" @click="handleDelete">
          {{ $t('kafka-group-delete') }}
        </Button>
      </div>
    </div>

    <div v-if="!groupId" class="kafka-group-panel__empty">
      {{ $t('kafka-group-select-hint') }}
    </div>

    <div v-else class="kafka-group-panel__body">
      <a-spin :spinning="loading">
        <div class="kafka-group-panel__section">
          <div class="kafka-group-panel__title">{{ $t('kafka-group-offsets', { group: groupId }) }}</div>
          <div class="kafka-group-panel__metrics">
            <div class="kafka-group-panel__metric">
              <span class="kafka-group-panel__metric-label">{{ $t('kafka-group-state') }}</span>
              <span class="kafka-group-panel__metric-value">{{ groupState || '-' }}</span>
            </div>
            <div class="kafka-group-panel__metric">
              <span class="kafka-group-panel__metric-label">{{ $t('kafka-group-members') }}</span>
              <span class="kafka-group-panel__metric-value">{{ memberCount ?? '-' }}</span>
            </div>
          </div>
          <a-table
            size="small"
            bordered
            :pagination="false"
            :scroll="{ y: 360 }"
            :columns="offsetColumns"
            :data-source="offsetRows"
            row-key="rowKey"
          />
        </div>
      </a-spin>
    </div>

    <Modal v-model="resetVisible" :title="$t('kafka-group-reset-offset')" :mask-closable="false" @on-ok="handleReset">
      <div class="kafka-group-panel__reset">
        <div class="kafka-group-panel__field">
          <div class="kafka-group-panel__field-label">{{ $t('kafka-group-reset-scope') }}</div>
          <Select v-model="resetForm.scope" style="width: 100%">
            <Option value="ALL">{{ $t('kafka-group-scope-all') }}</Option>
            <Option value="TOPIC">{{ $t('kafka-group-scope-topic') }}</Option>
            <Option value="PARTITION">{{ $t('kafka-group-scope-partition') }}</Option>
          </Select>
        </div>
        <div v-if="resetForm.scope !== 'ALL'" class="kafka-group-panel__field">
          <div class="kafka-group-panel__field-label">{{ $t('kafka-group-topic') }}</div>
          <Select v-model="resetForm.topic" filterable style="width: 100%">
            <Option v-for="topic in topicOptions" :key="topic" :value="topic">{{ topic }}</Option>
          </Select>
        </div>
        <div v-if="resetForm.scope === 'PARTITION'" class="kafka-group-panel__field">
          <div class="kafka-group-panel__field-label">{{ $t('kafka-topic-partition-id') }}</div>
          <InputNumber v-model="resetForm.partition" :min="0" :precision="0" style="width: 100%" />
        </div>
        <div class="kafka-group-panel__field">
          <div class="kafka-group-panel__field-label">{{ $t('kafka-group-reset-mode') }}</div>
          <Select v-model="resetForm.mode" style="width: 100%">
            <Option value="OFFSET">{{ $t('kafka-group-mode-offset') }}</Option>
            <Option value="BEGINNING">{{ $t('kafka-group-mode-beginning') }}</Option>
            <Option value="LATEST">{{ $t('kafka-group-mode-latest') }}</Option>
            <Option value="TIMESTAMP">{{ $t('kafka-group-mode-timestamp') }}</Option>
          </Select>
        </div>
        <div v-if="resetForm.mode === 'OFFSET'" class="kafka-group-panel__field">
          <div class="kafka-group-panel__field-label">{{ $t('kafka-group-offset-value') }}</div>
          <InputNumber v-model="resetForm.offset" :min="0" :precision="0" style="width: 100%" />
        </div>
        <div v-if="resetForm.mode === 'TIMESTAMP'" class="kafka-group-panel__field">
          <div class="kafka-group-panel__field-label">{{ $t('kafka-group-reset-datetime') }}</div>
          <DatePicker v-model="resetForm.datetime" type="datetime" style="width: 100%" />
        </div>
        <div class="kafka-group-panel__hint">{{ $t('kafka-group-reset-hint') }}</div>
      </div>
    </Modal>
  </div>
</template>

<script>
import CustomIcon from '@/components/function/CustomIcon.vue';

export default {
  name: 'KafkaGroupPanel',
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
      deleting: false,
      loadSeq: 0,
      groupState: '',
      memberCount: null,
      offsetRows: [],
      resetVisible: false,
      resetForm: {
        scope: 'ALL',
        topic: '',
        partition: 0,
        mode: 'LATEST',
        offset: 0,
        datetime: null
      }
    };
  },
  computed: {
    groupId() {
      const table = this.tab.selectedTable;
      if (!table) {
        return '';
      }
      return table.objName || table.title || '';
    },
    offsetColumns() {
      return [
        { title: this.$t('kafka-group-topic'), dataIndex: 'TOPIC', key: 'TOPIC' },
        { title: this.$t('kafka-topic-partition-id'), dataIndex: 'PARTITION', key: 'PARTITION', width: 90 },
        { title: this.$t('kafka-group-current-offset'), dataIndex: 'CURRENT_OFFSET', key: 'CURRENT_OFFSET', width: 140 },
        { title: this.$t('kafka-group-log-end'), dataIndex: 'LOG_END_OFFSET', key: 'LOG_END_OFFSET', width: 140 },
        { title: this.$t('kafka-group-lag'), dataIndex: 'LAG', key: 'LAG', width: 100 }
      ];
    },
    topicOptions() {
      const set = new Set();
      this.offsetRows.forEach((row) => {
        if (row.TOPIC) {
          set.add(row.TOPIC);
        }
      });
      return Array.from(set).sort();
    }
  },
  watch: {
    groupId: {
      immediate: true,
      handler(value) {
        if (value) {
          this.$nextTick(() => {
            this.loadGroupDetail();
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
      this.groupState = '';
      this.memberCount = null;
      this.offsetRows = [];
    },
    quoteIdent(value) {
      return `'${String(value).replace(/'/g, "''")}'`;
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
    normalizeRows(rows) {
      return (rows || []).map((row, index) => {
        const topic = this.rowValue(row, 'TOPIC');
        const partition = this.rowValue(row, 'PARTITION');
        return {
          STATE: this.rowValue(row, 'STATE'),
          MEMBER_COUNT: this.rowValue(row, 'MEMBER_COUNT'),
          TOPIC: topic,
          PARTITION: partition,
          CURRENT_OFFSET: this.rowValue(row, 'CURRENT_OFFSET'),
          LOG_END_OFFSET: this.rowValue(row, 'LOG_END_OFFSET'),
          LAG: this.rowValue(row, 'LAG'),
          rowKey: `${topic || ''}-${partition ?? index}`
        };
      });
    },
    async ensureSession() {
      if (!this.tab.sessionId) {
        await this.createSession(this.tab);
      }
      if (!this.tab.sessionId) {
        throw new Error(this.$t('kafka-group-load-failed'));
      }
    },
    async loadGroupDetail() {
      if (!this.groupId) {
        return;
      }
      const seq = ++this.loadSeq;
      this.loading = true;
      try {
        await this.ensureSession();
        if (seq !== this.loadSeq) {
          return;
        }
        const rows = await this.executeQuery(this.tab, `DESCRIBE GROUP ${this.quoteIdent(this.groupId)}`);
        if (seq !== this.loadSeq) {
          return;
        }
        const normalized = this.normalizeRows(rows);
        this.offsetRows = normalized.filter((row) => row.TOPIC);
        const first = normalized[0];
        this.groupState = first?.STATE || '';
        this.memberCount = first?.MEMBER_COUNT ?? null;
      } catch (error) {
        if (seq !== this.loadSeq) {
          return;
        }
        this.offsetRows = [];
        this.groupState = '';
        this.memberCount = null;
        this.$Message.error(error.message || this.$t('kafka-group-load-failed'));
      } finally {
        if (seq === this.loadSeq) {
          this.loading = false;
        }
      }
    },
    openResetModal() {
      if (!this.groupId) {
        return;
      }
      this.resetForm = {
        scope: 'ALL',
        topic: this.topicOptions[0] || '',
        partition: 0,
        mode: 'LATEST',
        offset: 0,
        datetime: new Date()
      };
      this.resetVisible = true;
    },
    async handleDelete() {
      if (!this.groupId) {
        return;
      }
      const groupId = this.groupId;
      this.$Modal.confirm({
        title: this.$t('kafka-group-delete'),
        content: this.$t('kafka-group-delete-confirm', { group: groupId }),
        onOk: async () => {
          this.deleting = true;
          try {
            await this.ensureSession();
            await this.executeQuery(this.tab, `DELETE GROUP ${this.quoteIdent(groupId)}`);
            this.$Message.success(this.$t('kafka-group-delete-success'));
            this.tab.selectedTable = null;
            this.resetState();
          } catch (error) {
            this.$Message.error(error.message || this.$t('kafka-group-delete-failed'));
          } finally {
            this.deleting = false;
          }
        }
      });
    },
    async handleReset() {
      if (!this.groupId) {
        return;
      }
      if (this.resetForm.scope !== 'ALL' && !this.resetForm.topic) {
        this.$Message.warning(this.$t('kafka-group-topic-required'));
        return false;
      }
      if (this.resetForm.mode === 'OFFSET' && (this.resetForm.offset == null || this.resetForm.offset < 0)) {
        this.$Message.warning(this.$t('kafka-group-offset-required'));
        return false;
      }
      if (this.resetForm.mode === 'TIMESTAMP' && !this.resetForm.datetime) {
        this.$Message.warning(this.$t('kafka-group-datetime-required'));
        return false;
      }
      let command = `ALTER GROUP ${this.quoteIdent(this.groupId)} RESET OFFSET TO `;
      if (this.resetForm.mode === 'BEGINNING') {
        command += 'BEGINNING';
      } else if (this.resetForm.mode === 'LATEST') {
        command += 'LATEST';
      } else if (this.resetForm.mode === 'TIMESTAMP') {
        command += `TIMESTAMP ${new Date(this.resetForm.datetime).getTime()}`;
      } else {
        command += String(Math.trunc(this.resetForm.offset));
      }
      if (this.resetForm.scope !== 'ALL') {
        command += ` TOPIC ${this.quoteIdent(this.resetForm.topic)}`;
      }
      if (this.resetForm.scope === 'PARTITION') {
        command += ` PARTITION ${Math.trunc(this.resetForm.partition)}`;
      }
      try {
        await this.ensureSession();
        await this.executeQuery(this.tab, command);
        this.$Message.success(this.$t('kafka-group-reset-success'));
        this.resetVisible = false;
        await this.loadGroupDetail();
      } catch (error) {
        this.$Message.error(error.message || this.$t('kafka-group-reset-failed'));
        return false;
      }
    }
  }
};
</script>

<style scoped lang="less">
.kafka-group-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--sql-workspace-panel-bg, #fff);
}

.kafka-group-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--border-color-base, #e8eaec);
}

.kafka-group-panel__connection {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.kafka-group-panel__actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.kafka-group-panel__empty {
  padding: 48px 16px;
  text-align: center;
  color: #808695;
}

.kafka-group-panel__body {
  padding: 12px;
  overflow: auto;
}

.kafka-group-panel__title {
  font-weight: 600;
  margin-bottom: 8px;
}

.kafka-group-panel__metrics {
  display: flex;
  gap: 16px;
  margin-bottom: 8px;
}

.kafka-group-panel__metric {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.kafka-group-panel__metric-label {
  color: #808695;
  font-size: 12px;
}

.kafka-group-panel__metric-value {
  font-weight: 600;
}

.kafka-group-panel__reset {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.kafka-group-panel__field-label {
  margin-bottom: 4px;
  color: #515a6e;
}

.kafka-group-panel__hint {
  color: #808695;
  font-size: 12px;
}
</style>
