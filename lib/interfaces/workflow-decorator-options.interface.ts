export interface WorkflowMetadata {
  name: string;
  version: number;
}

export interface WorkflowDecoratorOptions {
  /**
   * Bump when a change would not replay against journals written by the
   * previous code (renamed, removed, reordered or inserted steps). Keep the old
   * class registered until its instances finish. Default 1.
   */
  version?: number;
}
