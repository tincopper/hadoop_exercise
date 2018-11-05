package com.tomgs.hadoop.test.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationReportPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.EnumSet;
import java.util.List;

/**
 * 提交的任务信息管理
 *
 * @author tangzhongyuan
 * @create 2018-10-31 10:47
 **/
public class AppInfoManager {

    private YarnClient client;
    protected PrintStream sysout = System.out;
    private static final String APPLICATIONS_PATTERN = "%30s\t%20s\t%20s\t%10s\t%10s\t%18s\t%18s\t%15s\t%35s"
            + System.getProperty("line.separator");

    public static void main(String[] args) {
        try {
            AppInfoManager app = new AppInfoManager();
            app.testAppState();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void getJobState() {
        Configuration conf = new Configuration();
        client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
    }

    private void testAppState() throws YarnException, IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();

        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
            appStates.add(YarnApplicationState.ACCEPTED);
            appStates.add(YarnApplicationState.SUBMITTED);
        }
        List<ApplicationReport> appsReport = client.getApplications(appStates);

        PrintWriter writer = new PrintWriter(new OutputStreamWriter(sysout, Charset.forName("UTF-8")));
        for (ApplicationReport appReport : appsReport) {
            ApplicationReportPBImpl app = (ApplicationReportPBImpl) appReport;
            DecimalFormat formatter = new DecimalFormat("###.##%");
            String progress = formatter.format(appReport.getProgress());
            writer.printf(APPLICATIONS_PATTERN, appReport.getApplicationId(), appReport.getName(),
                    appReport.getApplicationType(), appReport.getUser(), appReport.getQueue(),
                    appReport.getYarnApplicationState(), appReport.getFinalApplicationStatus(), progress,
                    appReport.getOriginalTrackingUrl());
        }
        writer.flush();
        for (ApplicationReport appReport : appsReport) {
            String type = appReport.getApplicationType();
            if (type.equalsIgnoreCase("spark")) {
                continue;
            }
            getStatusByAppId(appReport);
        }

    }

    private void getStatusByAppId(ApplicationReport app) {
        String user = app.getUser();
        ApplicationId id = app.getApplicationId();
        String appId = app.getApplicationId().toString();
        System.out.println(appId);
    }

    private void killApplication(String applicationId) throws YarnException, IOException {
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        ApplicationReport appReport = null;
        try {
            appReport = client.getApplicationReport(appId);
        } catch (ApplicationNotFoundException e) {
            sysout.println("Application with id '" + applicationId +
                    "' doesn't exist in RM.");
            throw e;
        }

        if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
                || appReport.getYarnApplicationState() == YarnApplicationState.KILLED
                || appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
            sysout.println("Application " + applicationId + " has already finished ");
        } else {
            sysout.println("Killing application " + applicationId);
            client.killApplication(appId);
        }
    }

    private void getAppState() throws Exception {
        String[] args = {"-list"};
        ApplicationCLI.main(args);
    }
}
