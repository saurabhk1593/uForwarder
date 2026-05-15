package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.m3.tally.Scope;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

/**
 * Spring auto-configuration for consumer-lag mitigation.
 *
 * <p>Activated only when {@code master.lag.mitigation.enabled=true}. When active it wraps the
 * existing primary {@link Rebalancer} bean inside a {@link LagMitigationRebalancer} decorator so
 * that all existing rebalancing logic is preserved and lag mitigation is layered on top via {@link
 * LagMitigationRebalancer#postProcess}.
 *
 * <p>Example {@code application.properties}:
 *
 * <pre>
 * master.lag.mitigation.enabled=true
 * master.lag.mitigation.lagOffsetThreshold=50000
 * master.lag.mitigation.maxConcurrentCatchUpJobs=5
 * master.lag.mitigation.catchUpConsumerGroupSuffix=-catchup
 * </pre>
 */
@Configuration
@Profile("data-transfer-controller")
@EnableConfigurationProperties(LagMitigationConfiguration.class)
@ConditionalOnProperty(
    prefix = "master.lag.mitigation",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = false)
public class LagMitigationAutoConfiguration {

  @Bean
  public LagDetector lagDetector(LagMitigationConfiguration lagMitigationConfiguration) {
    return new LagDetector(lagMitigationConfiguration);
  }

  /**
   * Wraps the existing (delegate) {@link Rebalancer} bean with a {@link LagMitigationRebalancer}.
   * The {@code @Primary} annotation ensures this decorated bean wins whenever a single {@link
   * Rebalancer} is injected elsewhere in the application context.
   */
  @Bean
  @Primary
  public Rebalancer lagMitigationRebalancer(
      Rebalancer delegate,
      LagDetector lagDetector,
      LagMitigationConfiguration lagMitigationConfiguration,
      LeaderSelector leaderSelector,
      Scope scope) {
    return new LagMitigationRebalancer(
        delegate, lagDetector, lagMitigationConfiguration, leaderSelector, scope);
  }
}
