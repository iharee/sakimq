package com.arth.sakimq.config;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.source.yaml.YamlConfigSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * 分层配置：环境变量 > YAML 配置文件 > 默认值
 * 
 * YAML 文件路径由环境变量 {@code SAKIMQ_CONFIG} 指定，缺省为工作目录下的 {@code sakimq.yaml}。
 * 环境变量通过 SmallRye 的 EnvConfigSource 自动映射到属性名（{@code _} / {@code .} / {@code -} 等价、大小写不敏感）
 */
public final class Config {

    /** YAML 源的 ordinal，低于环境变量(300)，高于代码默认值。 */
    private static final int YAML_ORDINAL = 200;

    private final SmallRyeConfig config;

    private Config(SmallRyeConfig config) {
        this.config = config;
    }

    public static Config load() {
        String configPath = System.getenv("SAKIMQ_CONFIG");
        Path path = (configPath == null || configPath.isBlank())
                ? Path.of("sakimq.yaml")
                : Path.of(configPath);

        // 系统属性(400) > 环境变量(300) > YAML(200) > 默认值
        SmallRyeConfigBuilder builder = new SmallRyeConfigBuilder().addSystemSources();

        if (Files.exists(path)) {
            try {
                String content = Files.readString(path);
                builder.withSources(new YamlConfigSource("sakimq-yaml", content, YAML_ORDINAL));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read config file: " + path, e);
            }
        }

        return new Config(builder.build());
    }

    public String get(String key, String defaultValue) {
        return config.getOptionalValue(key, String.class).orElse(defaultValue);
    }

    public int getInt(String key, int defaultValue) {
        return config.getOptionalValue(key, Integer.class).orElse(defaultValue);
    }

    public Path getPath(String key, Path defaultValue) {
        return config.getOptionalValue(key, String.class).map(Path::of).orElse(defaultValue);
    }

    public Duration getDuration(String key, Duration defaultValue) {
        String value = config.getOptionalValue(key, String.class).orElse(null);
        return value == null ? defaultValue : parseDuration(value);
    }

    /**
     * 解析时长格式，支持的格式 {@code 500ms} / {@code 30s} / {@code 5m} / {@code 2h} / {@code 1d}；
     * 也支持 ISO-8601（如 {@code PT1H}）；无单位视为毫秒。
     */
    private static Duration parseDuration(String value) {
        String v = value.trim();
        if (v.isEmpty()) {
            throw new IllegalArgumentException("invalid duration: " + value);
        }
        if (v.charAt(0) == 'P' || v.charAt(0) == 'p') {
            return Duration.parse(v);
        }
        if (v.chars().allMatch(Character::isDigit)) {
            return Duration.ofMillis(Long.parseLong(v));
        }
        String lower = v.toLowerCase();
        try {
            if (lower.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(lower.substring(0, lower.length() - 2).trim()));
            }
            long amount = Long.parseLong(lower.substring(0, lower.length() - 1).trim());
            return switch (lower.charAt(lower.length() - 1)) {
                case 'd' -> Duration.ofDays(amount);
                case 'h' -> Duration.ofHours(amount);
                case 'm' -> Duration.ofMinutes(amount);
                case 's' -> Duration.ofSeconds(amount);
                default -> throw new IllegalArgumentException("invalid duration: " + value);
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid duration: " + value);
        }
    }
}