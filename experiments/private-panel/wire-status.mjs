// Narrow observer for this fixture's non-redirecting CDP requests.
export class WireStatuses {
  statuses = [];
  overflow = false;
  #entries = new Map();

  request(id, selected) { this.#record(id, { selected }); }
  response(id, status) { this.#record(id, { status }); }

  #record(id, update) {
    // ExtraInfo may precede requestWillBeSent. Retain both halves, including
    // unselected requests, so event order cannot erase a real refused response.
    if (!this.#entries.has(id) && this.#entries.size >= 32) {
      this.overflow = true;
      return;
    }
    const entry = this.#entries.get(id) ?? {};
    Object.assign(entry, update);
    this.#entries.set(id, entry);
    if (entry.selected === true && entry.status !== undefined && !entry.observed) {
      entry.observed = true;
      if (this.statuses.length >= 8) this.overflow = true;
      else this.statuses.push(entry.status);
    }
  }
}
