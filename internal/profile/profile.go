package profile

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const configFileName = "profiles.yaml"

var configDirFunc = configDir

type Profile struct {
	Name    string `yaml:"name"`
	ConnStr string `yaml:"conn_str"`
}

type Config struct {
	Default  string    `yaml:"default,omitempty"`
	Profiles []Profile `yaml:"profiles"`
}

func Resolve(name string) (string, error) {
	cfg, err := load()
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no profiles configured")
		}
		return "", err
	}

	for _, p := range cfg.Profiles {
		if p.Name == name {
			return p.ConnStr, nil
		}
	}

	return "", fmt.Errorf("profile %q not found", name)
}

func List() ([]Profile, error) {
	cfg, err := load()
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return cfg.Profiles, nil
}

func Add(name, connStr string) error {
	cfg, err := load()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if cfg == nil {
		cfg = &Config{}
	}

	for i, p := range cfg.Profiles {
		if p.Name == name {
			cfg.Profiles[i].ConnStr = connStr
			return save(cfg)
		}
	}

	cfg.Profiles = append(cfg.Profiles, Profile{
		Name:    name,
		ConnStr: connStr,
	})
	return save(cfg)
}

func Remove(name string) error {
	cfg, err := load()
	if err != nil {
		return err
	}

	for i, p := range cfg.Profiles {
		if p.Name == name {
			cfg.Profiles = append(cfg.Profiles[:i], cfg.Profiles[i+1:]...)
			if cfg.Default == name {
				cfg.Default = ""
			}
			return save(cfg)
		}
	}

	return fmt.Errorf("profile %q not found", name)
}

func ResolveConnStr(db, profileName string) (string, error) {
	if db != "" {
		return db, nil
	}
	if profileName != "" {
		return Resolve(profileName)
	}

	cfg, err := load()
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	if cfg.Default != "" {
		return Resolve(cfg.Default)
	}

	return "", nil
}

func load() (*Config, error) {
	path, err := configPath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	return &cfg, nil
}

func configDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("finding config directory: %w", err)
	}
	return filepath.Join(base, "pgplan"), nil
}

func configPath() (string, error) {
	dir, err := configDirFunc()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, configFileName), nil
}

func ensureConfigDir() error {
	dir, err := configDirFunc()
	if err != nil {
		return err
	}
	return os.MkdirAll(dir, 0700)
}

func save(cfg *Config) error {
	if err := ensureConfigDir(); err != nil {
		return err
	}

	path, err := configPath()
	if err != nil {
		return err
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("writing config %s: %w", path, err)
	}

	return nil
}

func SetDefault(name string) error {
	cfg, err := load()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if cfg == nil {
		cfg = &Config{}
	}

	found := false
	for _, p := range cfg.Profiles {
		if p.Name == name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("profile %q not found", name)
	}

	cfg.Default = name
	return save(cfg)
}

func ClearDefault() error {
	cfg, err := load()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	cfg.Default = ""
	return save(cfg)
}
