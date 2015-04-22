package com.linkedin.camus.sweeper;

import com.linkedin.camus.sweeper.utils.DateUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CamusSingleFolderSweeperPlannerTest {

  @Mock
  FileSystem mockedFs;

  @Mock
  FileStatus mockedFileStatus;

  @Mock
  ContentSummary mockedContentSummary;

  @Test
  public void testCreateSweeperJobProps() throws Exception {
    Path inputDir = new Path("inputDir");
    Path outputDir = new Path("outputDir");
    DateUtils dUtils = new DateUtils(new Properties());
    DateTime currentHour = dUtils.getCurrentHour();
    DateTimeFormatter hourFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    String hour = currentHour.minusHours(1).toString(hourFormatter);
    Path inputDirWithHour = new Path(inputDir, hour);
    Path outputDirWithHour = new Path(outputDir, hour);

    //inputDir should exist, but outputDir shouldn't.
    when(mockedFs.exists(inputDir)).thenReturn(true);
    when(mockedFs.exists(outputDirWithHour)).thenReturn(false);

    FileStatus[] fileStatuses = {mockedFileStatus};
    when(mockedFs.globStatus(any(Path.class))).thenReturn(fileStatuses);
    when(mockedFileStatus.getPath()).thenReturn(inputDirWithHour);

    long dataSize = 100;
    when(mockedContentSummary.getLength()).thenReturn(dataSize);
    when(mockedFs.getContentSummary(inputDirWithHour)).thenReturn(mockedContentSummary);

    String topic = "testTopic";

    List<Properties> jobPropsList =
        new CamusSingleFolderSweeperPlanner().setPropertiesLogger(new Properties(),
                                                                  Logger.getLogger("testLogger"))
            .createSweeperJobProps(topic, inputDir, outputDir, mockedFs);

    assertEquals(1, jobPropsList.size());

    Properties jobProps = jobPropsList.get(0);
    String topicAndHour = topic + ":" + hour;

    assertEquals(topic, jobProps.getProperty("topic"));
    assertEquals(topicAndHour, jobProps.getProperty(CamusSingleFolderSweeper.TOPIC_AND_HOUR));
    assertEquals(inputDirWithHour.toString(),
                 jobProps.getProperty(CamusSingleFolderSweeper.INPUT_PATHS));
    assertEquals(outputDirWithHour.toString(),
                 jobProps.getProperty(CamusSingleFolderSweeper.DEST_PATH));
  }
}
