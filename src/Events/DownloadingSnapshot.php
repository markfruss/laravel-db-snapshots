<?php

namespace Spatie\DbSnapshots\Events;

use Spatie\DbSnapshots\Snapshot;

class DownloadingSnapshot
{
    public Snapshot $snapshot;
    public string $snapshotFilePath;

    public function __construct(Snapshot $snapshot, string $snapshotFilePath)
    {
        $this->snapshot = $snapshot;
        $this->snapshotFilePath = $snapshotFilePath;
    }
}
