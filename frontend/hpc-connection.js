(() => {
  if (location.protocol === 'file:') return;

  const endpoint = '/api/v1/hpc-connection';
  const open = document.getElementById('hpc-connect-button');
  const dialog = document.getElementById('hpc-connect-dialog');
  const form = document.getElementById('hpc-connect-form');
  const token = document.getElementById('hpc-token');
  const status = document.getElementById('hpc-connect-status');
  const submit = document.getElementById('hpc-submit');
  const close = document.getElementById('hpc-close');
  const disconnect = document.getElementById('hpc-disconnect');
  const collection = document.getElementById('hpc-collection-enabled');
  const collectionStatus = document.getElementById('hpc-collection-status');
  const conflict = document.getElementById('collection-conflict-dialog');
  const description = document.getElementById('collection-conflict-description');
  const accept = document.getElementById('collection-conflict-accept');
  const decline = document.getElementById('collection-conflict-cancel');
  let collectionEnabled = null;
  open.disabled = true;

  function showConnection(data) {
    const demo = data.source_kind === 'synthetic';
    open.hidden = demo;
    open.disabled = demo;
    open.textContent = data.source_kind === 'hpc_api' ? '管理 HPC 连接' : '连接 SJTU HPC';
    disconnect.hidden = !data.connected;
    collectionEnabled = data.collection_enabled;
    collection.hidden = typeof collectionEnabled !== 'boolean';
    collection.textContent = collectionEnabled ? '中断后台采集' : '开启后台采集';
  }

  async function request(method, body, path = endpoint) {
    const response = await fetch(path, {method, cache: 'no-store', ...(body && {
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
    })});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'HPC 连接失败，请重试。');
    showConnection(data);
    return data;
  }

  async function setCollectionEnabled(enabled) {
    collection.disabled = true;
    collectionStatus.hidden = true;
    try {
      await request('POST', {enabled}, '/api/v1/live-collection');
      document.dispatchEvent(new CustomEvent('hpc-collection-changed', {detail: {enabled}}));
    } finally {
      collection.disabled = false;
    }
  }
  function confirmInterrupt(signal, {monitor = false} = {}) {
    if (signal.aborted || conflict.open) return Promise.resolve(false);
    description.textContent = monitor
      ? '中断后台采集后，将实时监控选中的作业。看板会保留最近一次已发布的结果。'
      : '这次查询需要使用 HPC 接口。中断采集后会继续查询，并保留最近一次已发布的结果。';
    accept.textContent = monitor ? '中断采集并监控' : '中断采集并查询';
    decline.textContent = monitor ? '仅查看作业详情' : '取消查询';
    conflict.returnValue = 'cancel';
    return new Promise(resolve => {
      const cancel = () => conflict.close('cancel');
      conflict.addEventListener('close', () => {
        signal.removeEventListener('abort', cancel);
        resolve(conflict.returnValue === 'interrupt' && !signal.aborted);
      }, {once: true});
      signal.addEventListener('abort', cancel, {once: true});
      conflict.showModal();
    });
  }

  window.hpcCollection = {setEnabled: setCollectionEnabled, getStatus: () => request('GET'), confirmInterrupt};

  async function changeConnection(method, body) {
    submit.disabled = close.disabled = disconnect.disabled = true;
    status.classList.remove('error');
    status.textContent = method === 'POST' ? '正在验证令牌并读取运行作业…' : '正在断开连接…';
    try {
      await request(method, body);
      token.value = '';
      dialog.close();
      document.dispatchEvent(new Event('hpc-source-changed'));
    } catch (error) {
      status.classList.add('error');
      status.textContent = error instanceof SyntaxError ? '连接服务返回格式异常，请重试。' : error.message;
    } finally {
      submit.disabled = close.disabled = disconnect.disabled = false;
    }
  }

  open.addEventListener('click', async () => {
    status.textContent = '';
    status.classList.remove('error');
    dialog.showModal();
    try {
      await request('GET');
    } catch (error) {
      status.textContent = error.message;
      status.classList.add('error');
    }
  });
  collection.addEventListener('click', async () => {
    try {
      await setCollectionEnabled(!collectionEnabled);
    } catch (error) {
      collectionStatus.textContent = error.message;
      collectionStatus.hidden = false;
    }
  });
  close.addEventListener('click', () => dialog.close());
  dialog.addEventListener('cancel', event => { if (submit.disabled) event.preventDefault(); });
  dialog.addEventListener('close', () => { token.value = ''; });
  form.addEventListener('submit', event => {
    event.preventDefault();
    const value = token.value.trim();
    if (!value) {
      token.value = '';
      token.reportValidity();
      return;
    }
    changeConnection('POST', {token: value});
  });
  disconnect.addEventListener('click', () => changeConnection('DELETE'));
  request('GET').then(data => {
    if (data.collection_enabled === false) return setCollectionEnabled(true);
  }).catch(error => {
    collectionStatus.textContent = error.message;
    collectionStatus.hidden = false;
  });
})();
