#pragma once

#include <event2/buffer.h>

#include <unordered_map>
#include <vector>
#include <phosg/Image.hh>

#include "../gen-cpp/Cyclone.h"
#include "Renderer.hh"


class ImageRenderer : public Renderer {
public:
  ImageRenderer(struct evbuffer* buf, Image::ImageFormat format);
  ImageRenderer() = delete;
  ImageRenderer(const ImageRenderer& rhs) = delete;
  const ImageRenderer& operator=(const ImageRenderer& rhs) = delete;
  virtual ~ImageRenderer() = default;

  const char* content_type() const;
  virtual void render_data(const std::unordered_map<std::string, ReadResult>& data) const;
  virtual void render_find_results(const std::unordered_map<std::string, FindResult>& data) const;

  void set_image_parameters(uint64_t width, uint64_t height, double y_min, double y_max);

private:

  Image::ImageFormat image_format;
  static uint8_t background_color[3];
  static uint8_t foreground_color[3];
  static std::vector<std::vector<uint8_t>> line_colors;

  uint64_t width;
  uint64_t height;
  double y_min_fixed;
  double y_max_fixed;
};
