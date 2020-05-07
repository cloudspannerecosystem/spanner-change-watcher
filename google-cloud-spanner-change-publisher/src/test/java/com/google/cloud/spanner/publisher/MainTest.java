package com.google.cloud.spanner.publisher;

import static com.google.cloud.spanner.publisher.Configuration.createConfiguration;
import static com.google.cloud.spanner.publisher.Configuration.prefix;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.watcher.SpannerDatabaseTailer;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class MainTest {

  static Properties createMinimalProperties() {
    Properties props = new Properties();
    props.put(prefix("spanner.instance"), "my-instance");
    props.put(prefix("spanner.database"), "my-database");
    props.put(prefix("pubsub.topicNameFormat"), "my-topic");
    return props;
  }

  static Properties createMinimalPropertiesExcept(String... keys) {
    Properties props = createMinimalProperties();
    for (String key : keys) {
      props.remove(key);
    }
    return props;
  }

  static Properties createMinimalPropertiesPlus(String key, String value, String... keysAndValues) {
    Preconditions.checkArgument(
        keysAndValues.length % 2 == 0, "keysAndValues must have even length");
    Properties props = createMinimalProperties();
    props.put(prefix(key), value);
    for (int i = 0; i < keysAndValues.length; i += 2) {
      props.put(prefix(keysAndValues[i]), keysAndValues[i + 1]);
    }
    return props;
  }

  @Test
  public void testCreateConfiguration() throws IOException {
    // Verify that the minimum required properties produce a valid configuration.
    assertThat(createConfiguration(createMinimalProperties())).isNotNull();

    Configuration config =
        createConfiguration(createMinimalPropertiesPlus("spanner.project", "my-project"));
    assertThat(config.getDatabaseId())
        .isEqualTo(DatabaseId.of("my-project", "my-instance", "my-database"));
    assertThat(config.isAllTables()).isTrue();
    assertThat(config.getPollInterval()).isEqualTo(Duration.ofMillis(100L));
    assertThat(config.getMaxWaitForShutdownSeconds()).isEqualTo(10L);

    // Configuration without anything is not supported.
    expectInvalid(new Properties());

    // Check required properties.
    expectInvalid(createMinimalPropertiesExcept(prefix("spanner.instance")));
    expectInvalid(createMinimalPropertiesExcept(prefix("spanner.database")));
    expectInvalid(createMinimalPropertiesExcept(prefix("pubsub.topicNameFormat")));

    config = createConfiguration(createMinimalPropertiesPlus("spanner.allTables", "true"));
    assertThat(config.isAllTables()).isTrue();
    // allTables=false requires includedTables.
    expectInvalid(createMinimalPropertiesPlus("spanner.allTables", "false"));
    // allTables=true + includedTables is not allowed.
    expectInvalid(
        createMinimalPropertiesPlus(
            "spanner.allTables", "true", "spanner.includedTables", "table1,table2"));
    // includedTables + excludedTables is not allowed.
    expectInvalid(
        createMinimalPropertiesPlus(
            "spanner.excludedTables", "table3", "spanner.includedTables", "table1,table2"));

    config =
        createConfiguration(
            createMinimalPropertiesPlus("spanner.includedTables", "table1,table2,table3"));
    assertThat(config.getIncludedTables()).asList().containsExactly("table1", "table2", "table3");
    config =
        createConfiguration(
            createMinimalPropertiesPlus("spanner.excludedTables", "table1,table2,table3"));
    assertThat(config.getExcludedTables()).asList().containsExactly("table1", "table2", "table3");
    config = createConfiguration(createMinimalPropertiesPlus("spanner.pollInterval", "PT1.5S"));
    assertThat(config.getPollInterval()).isEqualTo(Duration.ofMillis(1500L));
    config = createConfiguration(createMinimalPropertiesPlus("maxWaitForShutdownSeconds", "30"));
    assertThat(config.getMaxWaitForShutdownSeconds()).isEqualTo(30L);

    config =
        createConfiguration(createMinimalPropertiesPlus("pubsub.project", "some-pubsub-project"));
    assertThat(config.getPubsubProject()).isEqualTo("some-pubsub-project");
    config =
        createConfiguration(
            createMinimalPropertiesPlus("pubsub.topicNameFormat", "some-topic-name"));
    assertThat(config.getTopicNameFormat()).isEqualTo("some-topic-name");
  }

  @Test
  public void testCreateSpannerOptions() throws IOException {
    Configuration config =
        createConfiguration(MainTest.createMinimalPropertiesPlus("spanner.project", "my-project"));
    SpannerOptions options = Main.createSpannerOptions(config);
    assertThat(options.getProjectId()).isEqualTo("my-project");
  }

  @Test
  public void testCreateWatcher() throws IOException {
    Configuration config =
        createConfiguration(MainTest.createMinimalPropertiesPlus("spanner.project", "my-project"));
    Spanner spanner = mock(Spanner.class);
    SpannerDatabaseTailer watcher = (SpannerDatabaseTailer) Main.createWatcher(config, spanner);
    assertThat(watcher.getDatabaseId())
        .isEqualTo(DatabaseId.of("my-project", "my-instance", "my-database"));
    spanner.close();
  }

  @Test
  public void testCreatePublisher() throws IOException {
    Configuration config =
        createConfiguration(
            MainTest.createMinimalPropertiesPlus(
                "spanner.project",
                "my-spanner-project",
                "pubsub.project",
                "my-pubsub-project",
                "pubsub.topicNameFormat",
                "my-pubsub-topic"));
    Spanner spanner = mock(Spanner.class);
    SpannerDatabaseTailer watcher = (SpannerDatabaseTailer) Main.createWatcher(config, spanner);
    DatabaseId db = DatabaseId.of("my-spanner-project", "my-instance", "my-database");
    assertThat(watcher.getDatabaseId()).isEqualTo(db);
    Main.createPublisher(config, watcher, mock(DatabaseClient.class));
    spanner.close();
  }

  void expectInvalid(Properties properties) throws IOException {
    try {
      createConfiguration(properties);
      fail("missing expected exception");
    } catch (IllegalArgumentException e) {
    }
  }
}
