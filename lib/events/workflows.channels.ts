import { channel, type Channel } from 'node:diagnostics_channel';
import type { WorkflowEvent } from './workflow-events.interface.js';

const channels = new Map<WorkflowEvent['type'], Channel>();
export const channelFor = (type: WorkflowEvent['type']): Channel => {
  let target = channels.get(type);
  if (!target) {
    channels.set(type, (target = channel(`nestjs:workflows:${type}`)));
  }
  return target;
};
