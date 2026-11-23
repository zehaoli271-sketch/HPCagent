(() => {
  if (location.protocol === 'file:') return;

  const home = document.getElementById('home-view');
  const homeForm = document.getElementById('home-prompt');
  const homeQuestion = document.getElementById('home-question');
  const diagnosis = document.getElementById('diagnosis-view');
  const form = document.getElementById('diagnosis-form');
  const question = document.getElementById('diagnosis-question');
  const mode = document.getElementById('diagnosis-mode');
  const submit = document.getElementById('diagnosis-submit');
  const status = document.getElementById('diagnosis-status');
  const result = document.getElementById('diagnosis-result');
  const dashboardButton = document.getElementById('show-dashboard');
  const conversationButton = document.getElementById('show-conversation');
  const jobDialog = document.getElementById('chat-job-dialog');
  const jobTitle = document.getElementById('chat-job-title');
  const jobStatus = document.getElementById('chat-job-status');
  const jobContent = document.getElementById('chat-job-content');
  const homeTitle = document.title;
  const beijing = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai', dateStyle: 'short', timeStyle: 'medium', hourCycle: 'h23',
  });
  let pending;
  let pendingDetail;
  let chatId = crypto.randomUUID();

  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text != null) node.textContent = text;
    if (className) node.className = className;
    return node;
  }

  function table(headers, rows) {
    const viewport = element('div', null, 'diagnosis-table-wrap');
    const output = element('table', null, 'diagnosis-table');
    const head = output.createTHead().insertRow();
    headers.forEach(value => head.append(element('th', value)));
    const body = output.createTBody();
    rows.forEach(values => {
      const row = body.insertRow();
      values.forEach(value => {
        const cell = element('td');
        if (value instanceof Node) cell.append(value);
        else cell.textContent = value ?? '—';
        row.append(cell);
      });
    });
    viewport.append(output);
    return viewport;
  }

  function percent(value) {
    return value == null ? '待核实' : new Intl.NumberFormat('zh-CN', {
      style: 'percent', maximumFractionDigits: 2,
    }).format(value);
  }

  function jobButton(row, options = {}) {
    const button = element('button', row.job_id, 'chat-job-link');
    button.type = 'button';
    button.setAttribute('aria-haspopup', 'dialog');
    button.setAttribute('aria-label', `查看作业 ${row.job_id} 详情`);
    button.addEventListener('click', () => openJob(row, options));
    return button;
  }

  async function openJob(row, {pointAt, full, legacy, live} = {}) {
    pendingDetail?.abort();
    pendingDetail = null;
    jobTitle.textContent = `作业 ${row.job_id} 详情`;
    jobStatus.textContent = '';
    jobStatus.classList.remove('error');
    jobContent.replaceChildren();
    if (!jobDialog.open) jobDialog.showModal();
    if (live) {
      renderLiveDetail(row, jobContent);
      return;
    }
    if (full) {
      renderJobDetail(row, jobContent, false);
      return;
    }
    if (legacy) {
      renderJob(row, jobContent, false);
      return;
    }
    const controller = new AbortController();
    pendingDetail = controller;
    jobStatus.textContent = '正在读取作业和步骤记录…';
    try {
      const response = await fetch('/api/v1/job-detail', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({job_id: String(row.job_id), job_ref: row.job_ref, chat_id: chatId, point_at: pointAt}),
        signal: controller.signal,
      });
      if (response.status === 404) throw new Error('请重启本地服务，加载新版作业详情。');
      const data = await response.json();
      if (pendingDetail !== controller || !jobDialog.open) return;
      if (!response.ok) throw new Error(data.message || '暂时无法读取作业详情。');
      if (!data.details?.length) {
        jobStatus.textContent = data.message;
        list(jobContent, '数据说明', data.notes);
        return;
      }
      jobStatus.textContent = '';
      jobContent.append(element('p', `${data.source} · 查询于 ${displayTime(data.fetched_at)}`, 'diagnosis-source'));
      renderJobDetail(data.details[0], jobContent, false);
      list(jobContent, '数据说明', data.notes);
    } catch (error) {
      if (error.name === 'AbortError' || pendingDetail !== controller) return;
      jobStatus.classList.add('error');
      jobStatus.textContent = error instanceof TypeError ? '无法连接本地服务，请检查服务是否正在运行。' : error.message;
    } finally {
      if (pendingDetail === controller) pendingDetail = null;
    }
  }

  function renderScreening(screen, result) {
    const windowMode = screen.mode === 'window';
    const ranking = screen.top_n != null;
    const title = windowMode ? (ranking ? 'CPU 窗口排名' : 'CPU 窗口筛查') : '整段运行复盘';
    result.append(element('h3', title));
    const range = windowMode
      ? `${beijing.format(new Date(screen.window_start))} 至 ${beijing.format(new Date(screen.at))}`
      : `${beijing.format(new Date(screen.at))} 在运行的作业`;
    result.append(element('p', `${range}（北京时间）`, 'diagnosis-meta'));
    const coverage = `在运行 ${screen.running_count} 条 · 可评估 ${screen.evaluated_count} 条 · 数据不足 ${screen.unavailable_count} 条`;
    const outcome = ranking
      ? (screen.evaluated_count ? `已列出 ${screen.candidate_count} 条，目标前 ${screen.top_n} 条` : '暂无可排名的作业')
      : (screen.evaluated_count ? `复核候选 ${screen.candidate_count} 条 · 阈值低于 ${percent(screen.threshold)}` : '暂无可筛查的作业');
    result.append(element('p', `${coverage} · ${outcome}`, 'diagnosis-meta'));
    if (screen.cpu_source) result.append(element('p', `CPU 来源 · ${screen.cpu_source}`, 'diagnosis-meta'));
    if (!screen.candidates.length) return;
    const headers = ['作业 ID', '分区', windowMode ? '窗口 CPU 利用率' : '整段平均 CPU 利用率', '已用 / 分配（CPU 秒）'];
    if (ranking) headers.unshift('排名');
    if (windowMode) headers.push('实际观测时间');
    const rows = screen.candidates.map((row, index) => {
      const cells = [jobButton(row, {pointAt: Date.parse(screen.at) / 1000}), row.partition, percent(row.utilization),
        `${row.cpu_seconds} / ${row.capacity_cpu_seconds}`];
      if (ranking) cells.unshift(index + 1);
      if (windowMode) {
        cells.push(`${beijing.format(new Date(row.observed_start * 1000))} 至 ${beijing.format(new Date(row.observed_end * 1000))}`);
      }
      return cells;
    });
    result.append(table(headers, rows));
  }

  function renderJob(job, result, linked = true) {
    const heading = element('h3', linked ? '作业 ' : '');
    if (linked) heading.append(jobButton(job, {legacy: true}), ' · ');
    heading.append(`CPU 利用率 ${percent(job.cpu_efficiency.value)}`);
    result.append(heading);
    const quality = {verified: '已核对', unverified: '上游覆盖待核对', unavailable: '数据待核实'};
    result.append(element('p', [job.state, job.partition, quality[job.cpu_efficiency.quality]].filter(Boolean).join(' · '),
      'diagnosis-meta'));
    if (job.steps.length) {
      result.append(table(['步骤', '状态', '记录的 CPU 秒', '单任务 MaxRSS（字节）'],
        job.steps.map(step => [step.step_id, step.state, step.cpu_accounting_pending ? '尚未记账' : step.cpu_time_seconds, step.max_rss_bytes])));
    }
  }

  function renderLegacy(data, result, prompt) {
    if (data.data_source) {
      const kind = data.data_source.kind === 'synthetic' ? '合成演示数据' : '交大 HPC API';
      result.append(element('p', `${kind} · 查询于 ${beijing.format(new Date(data.data_source.queried_at))}（北京时间）`,
        'diagnosis-source'));
    }
    if (data.screening) renderScreening(data.screening, result);
    if (data.job) renderJob(data.job, result);
    if (data.history_coverage) {
      result.append(element('p', coverageText(data.history_coverage), 'diagnosis-meta'));
      historyRetryButton(data.history_coverage, result, prompt);
    }
    for (const [name, lines] of [['计算依据', data.diagnosis?.facts], ['数据缺口', data.diagnosis?.data_gaps]]) {
      if (!lines?.length) continue;
      result.append(element('h4', name));
      const list = element('ul');
      lines.forEach(line => list.append(element('li', line)));
      result.append(list);
    }
    if (data.screening?.mode === 'window' && !data.screening.evaluated_count && data.screening.running_count) {
      const fallback = element('button', '改查整段运行复盘', 'refresh');
      fallback.type = 'button';
      fallback.addEventListener('click', () => { mode.value = 'lifetime'; run(prompt); });
      result.append(fallback);
    }
    if (data.explanation) {
      result.append(element('h4', '致远一号解读'), element('p', data.explanation));
    }
  }

  const number = new Intl.NumberFormat('zh-CN', {maximumFractionDigits: 2});
  const displayNumber = value => value == null ? '待核实' : number.format(value);
  const displayTime = value => value ? beijing.format(new Date(value)) : '待核实';
  const displayGib = value => value == null ? '待采集' : `${displayNumber(value / (1024 ** 3))} GiB`;
  const fields = {
    submit: '提交时间', start: '开始时间', end: '结束时间', state: '状态',
    allocated_cpus: '分配 CPU（核）', requested_memory_mb: '申请内存（MB）',
    task_max_rss_bytes: '单任务 MaxRSS', seff_memory_bytes: 'seff 内存估算',
    job_memory_peak_bytes: '作业级并发内存峰值', allocated_memory_mb: '分配内存（MB）',
    active_seconds: '有效运行时间（秒）', allocated_nodes: '分配节点', cpu_seconds: '已用 CPU 秒',
    cpu_capacity: '分配 CPU 秒', count: '作业数',
  };

  function list(target, title, lines) {
    if (!lines?.length) return;
    target.append(element('h4', title));
    const output = element('ul');
    lines.forEach(line => output.append(element('li', line)));
    target.append(output);
  }

  function scopeText(scope) {
    if (!scope) return '';
    const inclusiveCutoff = scope.scope_note === 'completion_cutoff';
    const end = inclusiveCutoff ? new Date(Date.parse(scope.end_time) - 1000) : scope.end_time;
    const parts = [scope.start_time
      ? `${displayTime(scope.start_time)} 至 ${displayTime(end)}（北京时间，${inclusiveCutoff ? '含截止时刻' : '含开始、不含结束'}）`
      : '查询已收录的记录'];
    if (scope.scope_note) parts.push('从当天零点开始统计');
    if (scope.time_basis_label) parts.push(scope.time_basis_label);
    if (scope.states?.length) parts.push(scope.states.join(' / '));
    if (scope.partition) parts.push(`分区 ${scope.partition}`);
    scope.resource_filters?.forEach(item => parts.push(`${fields[item.field]} ${item.op} ${item.value}`));
    if (scope.threshold != null) parts.push(`阈值低于 ${percent(scope.threshold)}`);
    return parts.join(' · ');
  }

  function coverageText(coverage) {
    if (!coverage) return '';
    if (coverage.window_count) {
      const range = `${displayTime(coverage.first_capture)} 至 ${displayTime(coverage.last_capture)}`;
      const captured = coverage.requested_seconds == null ? `${coverage.captured_seconds} 秒`
        : `${coverage.captured_seconds} / ${coverage.requested_seconds} 秒`;
      return `采集范围 ${range} · ${coverage.window_count} 个成功采集窗口 · 采样覆盖 ${captured}`;
    }
    if (coverage.windows_total) {
      const gaps = coverage.timed_out_windows?.length ? ` · 超时 ${coverage.timed_out_windows.length} 个` : '';
      const reused = coverage.windows_reused ? ` · 复用 ${coverage.windows_reused} 个` : '';
      const versions = coverage.observed_versions == null ? '' : ` · 作业观测版本 ${coverage.observed_versions}`;
      return `HPC 窗口已完成 ${coverage.windows_completed} / ${coverage.windows_total}${gaps}${reused}${versions}`;
    }
    return '';
  }

  function historyRetryButton(coverage, target, prompt = null) {
    const retryable = ['hpc_timeout', 'hpc_query_budget', 'hpc_upstream_unavailable', 'hpc_rate_limited', 'hpc_incomplete_response'];
    if (!retryable.includes(coverage?.error?.code)) return;
    const button = element('button', prompt ? '重试此时刻' : '继续查询剩余窗口', 'refresh');
    button.type = 'button';
    button.addEventListener('click', () => {
      if (target.parentElement === result.lastElementChild) run(prompt || '继续查询');
    });
    target.append(button);
  }

  function renderAggregate(target, aggregate) {
    if (!aggregate) return;
    target.append(element('p', `匹配 ${aggregate.count} 条 · CPU 可评估 ${aggregate.cpu_evaluated} 条 / 待核实 ${aggregate.cpu_missing} 条 · 内存可评估 ${aggregate.memory_evaluated} 条 / 待核实 ${aggregate.memory_missing} 条`, 'diagnosis-meta'));
    if (!aggregate.count) return;
    target.append(table(['作业状态', '数量', '占比'], aggregate.states.map(row => [row.state, row.count, percent(row.ratio)])));
    const metrics = [
      ['申请 / 分配 CPU（跨作业核数之和）', `${displayNumber(aggregate.cpu_requested)} / ${displayNumber(aggregate.cpu_allocated)}`,
        `${aggregate.cpu_requested_samples} / ${aggregate.cpu_allocated_samples}`],
      ['使用 / 容量（CPU 秒）', `${displayNumber(aggregate.cpu_seconds)} / ${displayNumber(aggregate.cpu_capacity)}`, aggregate.cpu_evaluated],
      ['申请 / 分配内存（MB 之和）', `${displayNumber(aggregate.memory_requested_mb)} / ${displayNumber(aggregate.memory_allocated_mb)}`,
        `${aggregate.memory_requested_samples} / ${aggregate.memory_allocated_samples}`],
      ['seff 内存估算之和（字节）', displayNumber(aggregate.seff_memory_bytes_total), aggregate.seff_memory_samples],
      ['作业级并发内存峰值之和（字节）', displayNumber(aggregate.peak_memory_bytes_total), aggregate.job_memory_peak_samples],
      ['有步骤单任务峰值的作业', aggregate.task_rss_samples, aggregate.count],
    ];
    target.append(table(['资源口径', '数值', '有效作业样本'], metrics));
  }

  function renderReport(data, target, prompt) {
    target.append(element('p', data.message, 'diagnosis-answer'));
    if (data.live) {
      renderLiveReport(data, target);
      return;
    }
    if (Object.hasOwn(data, 'schema_version')) {
      renderLegacy(data, target, prompt);
      return;
    }
    if (data.source) target.append(element('p', `${data.source} · 查询于 ${displayTime(data.fetched_at)}（北京时间）`, 'diagnosis-source'));
    if (data.scope) target.append(element('p', scopeText(data.scope), 'diagnosis-meta'));
    const coverage = coverageText(data.coverage);
    if (coverage) target.append(element('p', coverage, 'diagnosis-meta'));
    if (data.coverage?.event_bounds) {
      const labels = {submission: '提交时间', start: '开始时间', end: '结束时间'};
      target.append(table(['归档事件', '最早（北京时间）', '最晚（北京时间）'],
        Object.entries(data.coverage.event_bounds).map(([basis, range]) =>
          [labels[basis], displayTime(range.earliest), displayTime(range.latest)])));
    }
    if (data.coverage?.archive_modified_at) {
      target.append(element('p', `本地文件更新于 ${displayTime(data.coverage.archive_modified_at)}`, 'diagnosis-meta'));
    }
    renderAggregate(target, data.aggregate);
    if (data.screening_summary) {
      const screen = data.screening_summary;
      const waste = screen.metric === 'cpu'
        ? `候选未用容量合计 ${displayNumber(screen.unused_cpu_seconds)} CPU 秒`
        : `候选申请量与${screen.memory_basis === 'seff' ? ' seff 内存估算' : '并发峰值'}的差额合计 ${displayNumber(screen.memory_slack_bytes)} 字节`;
      target.append(element('p', `${screen.candidates} 条候选 · ${waste}`, 'diagnosis-meta'));
    }
    if (data.days?.length) target.append(table(['日期', '本次查到的作业数'], data.days.map(row => [row.date, row.count])));
    if (data.comparison) {
      const baseline = data.comparison;
      target.append(element('h4', '对照时间段'),
        element('p', `${baseline.source} · ${scopeText(baseline.scope)}`, 'diagnosis-meta'));
      const baselineCoverage = coverageText(baseline.coverage);
      if (baselineCoverage) target.append(element('p', baselineCoverage, 'diagnosis-meta'));
      renderAggregate(target, baseline.aggregate);
      if (baseline.differences) target.append(table(['本次匹配样本差值', '主区间减对照区间'], Object.entries(baseline.differences)
        .map(([key, value]) => [fields[key], displayNumber(value)])));
      list(target, '对照数据说明', baseline.notes);
    }
    if (data.quality) {
      const quality = data.quality;
      target.append(table(['质量检查', '作业数'], [
        [`有步骤记录（${percent(quality.steps_ratio)}）`, quality.steps_present], ['步骤记录出现 0', quality.zero_recorded],
        ['从未开始标记', quality.never_started], ['排队、运行中或记账未定稿', quality.accounting_pending],
        ...Object.entries(quality.cpu_reasons), ...Object.entries(quality.missing_fields)
          .map(([field, count]) => [`${fields[field]}缺失`, count]),
      ]));
    }
    if (data.records?.length) {
      const memoryHeader = data.records[0].memory_basis === 'seff' ? 'seff 估算 / 申请内存' : '并发峰值 / 申请内存';
      target.append(table(['序号', '作业 ID', '状态', '分区', 'CPU 利用率', memoryHeader, '有效运行秒数'],
        data.records.map((row, index) => [data.offset + index + 1, jobButton(row), row.state, row.partition,
          percent(row.cpu_ratio), percent(row.memory_ratio), row.active_seconds])));
      if (data.pages > 1) renderPagination(data, target);
    }
    if (data.point_evidence) renderScreening(data.point_evidence, target);
    const comparisonJobs = data.details?.length > 1 ? data.details : data.peer_reference ? [data.peer_reference] : [];
    if (comparisonJobs.length) target.append(table(['作业对照', '分区', 'CPU 利用率', '内存估算或并发峰值 / 申请', '有效运行秒数'],
      comparisonJobs.map(row => [jobButton(row, {full: data.details?.includes(row)}),
        row.partition, percent(row.cpu_ratio), percent(row.memory_ratio), row.active_seconds])));
    for (const row of data.details ?? []) renderJobDetail(row, target);
    list(target, '数据说明', data.notes);
    historyRetryButton(data.coverage, target);
  }

  function renderLiveReport(data, target) {
    if (data.fetched_at) target.append(element('p', `${data.source} · 采集于 ${displayTime(data.fetched_at)}（北京时间）`, 'diagnosis-source'));
    if (data.coverage.catalog_count != null) {
      const coverage = data.coverage;
      target.append(element('p', `快照覆盖 · 运行记录 ${coverage.catalog_count} 条 · CPU 可评估 ${coverage.cpu_evaluated} 条 · 内存可评估 ${coverage.memory_evaluated} 条 · 本轮读取节点 ${coverage.nodes_visited} / ${coverage.nodes_total}`, 'diagnosis-meta'));
    }
    if (data.snapshot_count) target.append(element('p', `近期保留 ${data.snapshot_count} 份内存快照。`, 'diagnosis-meta'));
    if (data.live_detail) {
      const title = element('h4', '运行作业 ');
      title.append(jobButton(data.live_detail, {live: true}));
      target.append(title);
      renderLiveDetail(data.live_detail, target);
    } else if (data.records?.length) {
      target.append(table(['序号', '作业 ID', '分区', 'CPU 窗口效率', '观测内存用量', '内存用量 / 分配'],
        data.records.map((row, index) => [data.offset + index + 1, jobButton(row, {live: true}), row.partition,
          percent(row.efficiency), displayGib(row.memory_used_bytes), percent(row.memory_ratio)])));
      if (data.pages > 1) renderPagination(data, target);
    }
    list(target, '数据说明', data.notes);
  }

  function renderLiveDetail(row, target) {
    target.append(table(['指标', '值'], [
      ['状态', row.state], ['分区', row.partition], ['开始时间', displayTime(row.started_at)],
      ['分配节点', row.nodes?.join('、') || '待核实'], ['分配 / 申请 CPU 核数', `${displayNumber(row.cpus)} / ${displayNumber(row.requested_cpus)}`],
      ['CPU 窗口效率', percent(row.efficiency)],
      ['CPU 实测起止', row.cpu_observed_end ? `${displayTime(row.cpu_observed_start)} 至 ${displayTime(row.cpu_observed_end)}` : '待采集'],
      ['实测窗口已用 / 分配容量（CPU 秒）', `${displayNumber(row.cpu_seconds)} / ${displayNumber(row.cpu_capacity)}`],
      ['对齐后的内存用量 / 分配量', `${displayGib(row.memory_used_bytes)} / ${displayGib(row.memory_allocated_bytes)}`],
      ['内存申请量', displayGib(row.requested_memory_bytes)],
      ['内存参考时间', displayTime(row.memory_observed_at)], ['节点样本最大时间差（秒）', displayNumber(row.memory_max_skew_seconds)],
    ]));
    if (row.reason) target.append(element('p', row.reason, 'diagnosis-meta'));
    list(target, '复核建议', row.advice);
  }

  function renderPagination(data, target) {
    const paging = element('p', `第 ${data.page} / ${data.pages} 页 `, 'diagnosis-meta');
    for (const [label, page] of [['上一页', data.page - 1], ['下一页', data.page + 1]]) {
      if (page < 1 || page > data.pages) continue;
      const button = element('button', label, 'refresh');
      button.type = 'button';
      button.addEventListener('click', () => {
        if (target.parentElement === result.lastElementChild) run(`第 ${page} 页`);
      });
      paging.append(button);
    }
    target.append(paging);
  }

  function renderJobDetail(row, target, linked = true) {
    if (linked) {
      const heading = element('h4', '作业 ');
      heading.append(jobButton(row, {full: true}));
      target.append(heading);
    }
    target.append(element('p', [row.state, row.partition, row.cluster, row.state_reason,
      row.exit_code != null ? `退出码 ${row.exit_code}` : null, row.observations != null ? `观测 ${row.observations} 次` : null]
      .filter(Boolean).join(' · '), 'diagnosis-meta'));
    target.append(table(['提交', '开始', '结束', '有效运行秒数', '分配 / 申请 CPU 核数', '分配 / 申请内存（MB）',
      'seff 内存估算（字节）', '并发内存峰值（字节）'],
      [[displayTime(row.submit), displayTime(row.start), displayTime(row.end), row.active_seconds,
        `${displayNumber(row.allocated_cpus)} / ${displayNumber(row.requested_cpus)}`,
        `${displayNumber(row.allocated_memory_mb)} / ${displayNumber(row.requested_memory_mb)}`,
        row.seff_memory_bytes, row.job_memory_peak_bytes]]));
    if (row.seff_memory_bytes != null && !row.seff_memory_complete) {
      target.append(element('p', '部分步骤缺少有效内存记账，当前 seff 估算仅来自已记录的步骤。', 'diagnosis-meta'));
    }
    if (row.steps?.length) {
      target.append(table(['步骤', '状态', 'CPU 秒', '单任务 MaxRSS（字节）', '任务峰值之和（字节）'],
        row.steps.map(step => [step.step_id ?? step.kind, step.state,
          step.cpu_accounting_pending ? '尚未记账' : step.cpu_seconds, step.task_max_rss_bytes, step.step_memory_total_bytes])));
    }
    list(target, '诊断依据', row.facts);
    list(target, '待核实的可能原因', row.hypotheses);
    list(target, '人工复核建议', row.suggestions);
  }

  async function run(value = question.value) {
    if (pending) return;
    const prompt = value.trim();
    if (!prompt) {
      question.value = '';
      question.reportValidity();
      return;
    }
    const controller = new AbortController();
    pending = controller;
    const turn = element('article', null, 'diagnosis-turn');
    const answer = element('div', null, 'diagnosis-turn-answer');
    const narrative = element('p', null, 'diagnosis-narrative');
    turn.append(element('p', prompt, 'diagnosis-user-message'), answer);
    result.append(turn);
    result.hidden = false;
    question.value = '';
    status.classList.remove('error');
    status.textContent = '正在发送查询…';
    submit.disabled = true;
    try {
      const response = await fetch('/api/v1/chat', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({question: prompt, mode: mode.value, chat_id: chatId}),
        signal: controller.signal,
      });
      if (!response.ok) throw new Error((await response.json()).message || '诊断请求失败。');
      if (!response.headers.get('content-type')?.startsWith('application/x-ndjson') || !response.body) {
        throw new Error('当前服务不支持对话流，请确认本地服务使用的是新版代码。');
      }
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let complete = false;
      let rendered = false;
      while (pending === controller) {
        const {done, value: chunk} = await reader.read();
        if (done || pending !== controller) break;
        buffer += decoder.decode(chunk, {stream: true});
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (!line) continue;
          const event = JSON.parse(line);
          if (event.type === 'progress') {
            const failed = event.timed_out ? `，超时 ${event.timed_out} 个` : '';
            const progress = event.total == null ? '' : `（已完成 ${event.completed} / ${event.total} 窗口${failed}）`;
            status.textContent = event.message + progress;
          } else if (event.type === 'result') {
            renderReport(event.data, answer, prompt);
            rendered = true;
          } else if (event.type === 'delta') {
            if (!narrative.isConnected) {
              answer.append(element('h4', '致远一号解读'), narrative);
            }
            narrative.append(document.createTextNode(event.text));
          } else if (event.type === 'notice') {
            answer.append(element('p', event.message, 'diagnosis-meta'));
          } else if (event.type === 'done') {
            complete = true;
          }
        }
      }
      if (pending !== controller) return;
      if (!complete) throw new Error(rendered ? '连接提前结束，已收到的查询结果已保留。' : '查询连接中断，请重试。');
      status.textContent = '';
    } catch (error) {
      if (error.name !== 'AbortError' && pending === controller) {
        status.classList.add('error');
        const message = error instanceof SyntaxError ? '服务返回格式异常，请重试。' : error.message;
        status.textContent = message;
        answer.append(element('p', message, 'diagnosis-meta'));
      }
    } finally {
      if (pending === controller) {
        pending = null;
        submit.disabled = false;
      }
    }
  }

  function showView(showDiagnosis, focus = true) {
    if (home.hidden === showDiagnosis) return;
    home.hidden = showDiagnosis;
    diagnosis.hidden = !showDiagnosis;
    dashboardButton.setAttribute('aria-pressed', String(!showDiagnosis));
    conversationButton.setAttribute('aria-pressed', String(showDiagnosis));
    document.title = showDiagnosis ? '作业诊断 · HPC Guardian' : homeTitle;
    window.scrollTo(0, 0);
    if (focus) document.getElementById(showDiagnosis ? 'diagnosis-title' : 'home-title').focus({preventScroll: true});
    if (!showDiagnosis) {
      if (jobDialog.open) jobDialog.close();
      document.dispatchEvent(new Event('heatmap-restored'));
    }
  }

  function selectView(showDiagnosis) {
    if (home.hidden === showDiagnosis) return;
    history.pushState(null, '', showDiagnosis ? '#diagnostics' : location.pathname + location.search);
    showView(showDiagnosis);
  }

  dashboardButton.addEventListener('click', () => selectView(false));
  conversationButton.addEventListener('click', () => selectView(true));
  window.addEventListener('hashchange', () => showView(location.hash === '#diagnostics'));
  showView(location.hash === '#diagnostics', false);
  homeForm.addEventListener('submit', event => {
    event.preventDefault();
    const prompt = homeQuestion.value.trim();
    if (!prompt) {
      homeQuestion.value = '';
      homeQuestion.reportValidity();
      return;
    }
    question.value = prompt;
    mode.value = 'auto';
    selectView(true);
    run();
  });
  form.addEventListener('submit', event => { event.preventDefault(); run(); });
  jobDialog.addEventListener('close', () => {
    pendingDetail?.abort();
    pendingDetail = null;
  });
  document.addEventListener('hpc-source-changed', () => {
    pending?.abort();
    pending = null;
    pendingDetail?.abort();
    pendingDetail = null;
    if (jobDialog.open) jobDialog.close();
    chatId = crypto.randomUUID();
    submit.disabled = false;
    status.classList.remove('error');
    status.textContent = '数据源已切换，可重新查询。';
    result.replaceChildren();
    result.hidden = true;
  });
})();
