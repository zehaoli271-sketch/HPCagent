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
  open.disabled = true;

  function showConnection(data) {
    const demo = data.source_kind === 'synthetic';
    open.hidden = demo;
    open.disabled = demo;
    open.textContent = data.source_kind === 'hpc_api' ? '管理 HPC 连接' : '连接 SJTU HPC';
    disconnect.hidden = !data.connected;
  }

  async function request(method, body) {
    const response = await fetch(endpoint, {method, cache: 'no-store', ...(body && {
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
    })});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'HPC 连接失败，请重试。');
    showConnection(data);
  }

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

  open.addEventListener('click', () => {
    status.textContent = '';
    status.classList.remove('error');
    dialog.showModal();
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
  request('GET').catch(() => {});
})();
