package org.hubspot.utils;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hubspot.utils.concurrent.CustomThreadFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.software.os.OSProcess;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Nicholas Curl
 */
public class CPUMonitor {

    /**
     * The instance of the logger
     */
    private static final Logger                   logger                   = LogManager.getLogger(CPUMonitor.class);
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new CustomThreadFactory("CPUMonitor"));
    private static final long                     PERIOD                   = 100;
    private static final AtomicDouble             processLoad              = new AtomicDouble();
    private static final AtomicDouble             systemLoad               = new AtomicDouble();
    private static final SystemInfo               si                       = new SystemInfo();
    private static final long                     pid                      = ProcessHandle.current().pid();
    private static final CentralProcessor         cpu                      = si.getHardware().getProcessor();
    private static       OSProcess                priorProcSnap;
    private static       long[]                   oldTicks                 = new long[TickType.values().length];

    public static double getProcessLoad() {
        return processLoad.get();
    }

    public static double getSystemLoad() {
        return systemLoad.get();
    }

    public static void startMonitoring() {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            OSProcess currentProcess = si.getOperatingSystem().getProcess((int) pid);
            double cpuLoad = Utils.round(100d * cpu.getSystemCpuLoadBetweenTicks(oldTicks), 1);
            double procLoad = Utils.round(100d * currentProcess.getProcessCpuLoadBetweenTicks(priorProcSnap) /
                                          cpu.getLogicalProcessorCount(), 1);
            processLoad.set(procLoad);
            priorProcSnap = currentProcess;
            oldTicks = cpu.getSystemCpuLoadTicks();
            systemLoad.set(cpuLoad);
        }, 0, PERIOD, TimeUnit.MILLISECONDS);
    }

    public static void stopMonitoring() {
        scheduledExecutorService.shutdownNow();
    }
}
