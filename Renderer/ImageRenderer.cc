#include "ImageRenderer.hh"

#include <math.h>

#include <algorithm>
#include <phosg/Image.hh>
#include <unordered_map>

using namespace std;


ImageRenderer::ImageRenderer(struct evbuffer* buf, Image::ImageFormat format)
    : Renderer(buf), image_format(format), width(800), height(600),
    y_min_fixed(nan("")), y_max_fixed(nan("")) { }

const char* ImageRenderer::content_type() const {
  return Image::mime_type_for_format(this->image_format);
}

void ImageRenderer::render_data(const unordered_map<string, ReadResult>& results) const {

  // sanity checks
  if (results.size() == 0) {
    throw runtime_error("no data provided to renderer");
  }
  if (results.begin()->second.data.size() == 0) {
    throw runtime_error("first series contains no data");
  }

  // compute axis bounds
  int64_t x_min = results.begin()->second.data.front().timestamp;
  int64_t x_max = results.begin()->second.data.back().timestamp;
  double y_min = this->y_min_fixed;
  double y_max = this->y_max_fixed;
  bool compute_y_min = isnan(y_min_fixed), compute_y_max = isnan(y_max_fixed);
  for (const auto& it : results) {
    const auto& data = it.second.data;

    if (data.size() == 0) {
      throw runtime_error("series [" + it.first + "] contains no data");
    }

    x_min = min(x_min, data[0].timestamp);
    x_max = max(x_max, data[data.size() - 1].timestamp);

    for (const auto& pt : data) {
      if (compute_y_min && (isnan(y_min) || pt.value < y_min)) {
        y_min = pt.value;
      }
      if (compute_y_max && (isnan(y_max) || pt.value > y_max)) {
        y_max = pt.value;
      }
    }
  }
  if (isnan(y_min) || isnan(y_max) || y_max <= y_min) {
    throw runtime_error("vertical axis boundaries are invalid");
  }
  double y_range = y_max - y_min;
  int64_t x_range = x_max - x_min;
  if (x_range <= 0) {
    throw runtime_error("horizontal axis boundaries are invalid");
  }

  Image img(width, height);
  img.clear(background_color[0], background_color[1], background_color[2]);

  int top_axis_label_width, bottom_axis_label_width;
  img.draw_text(10, 7, &top_axis_label_width, NULL, foreground_color[0], foreground_color[1], foreground_color[2], 0, 0, 0, 0, "%g", y_max);
  img.draw_text(10, height - 13, &bottom_axis_label_width, NULL, foreground_color[0], foreground_color[1], foreground_color[2], 0, 0, 0, 0, "%g", y_min);
  int axis_label_width = max(top_axis_label_width, bottom_axis_label_width);

  // check that the image size is reasonable
  uint64_t graph_left_edge = axis_label_width + 20;
  uint64_t graph_right_edge = width - 10;
  uint64_t graph_top_edge = 10;
  uint64_t graph_bottom_edge = height - 10;
  if (graph_left_edge >= graph_right_edge || graph_top_edge >= graph_bottom_edge) {
    throw runtime_error("image dimensions are too small");
  }
  uint64_t graph_width = graph_right_edge - graph_left_edge;
  uint64_t graph_height = graph_bottom_edge - graph_top_edge;

  // create image & draw boundaries
  img.draw_vertical_line(graph_left_edge, graph_top_edge, graph_bottom_edge, 0, foreground_color[0], foreground_color[1], foreground_color[2]);
  img.draw_vertical_line(graph_right_edge, graph_top_edge, graph_bottom_edge, 0, foreground_color[0], foreground_color[1], foreground_color[2]);
  img.draw_horizontal_line(graph_left_edge, graph_right_edge, graph_top_edge, 0, foreground_color[0], foreground_color[1], foreground_color[2]);
  img.draw_horizontal_line(graph_left_edge, graph_right_edge, graph_bottom_edge, 0, foreground_color[0], foreground_color[1], foreground_color[2]);

  // draw lines
  int current_color = 0;
  for (const auto& it : results) {
    int prev_x = 0, prev_y = 0;
    uint8_t* color = line_colors[current_color].data();
    for (const auto& pt : it.second.data) {
      int64_t this_x = (int64_t)(pt.timestamp - x_min) * graph_width / x_range + graph_left_edge;
      int64_t this_y = (y_min + y_range - pt.value) * graph_height / y_range + graph_top_edge;
      if (prev_x && prev_y) {
        img.draw_line(prev_x, prev_y, this_x, this_y, color[0], color[1], color[2]);
      }
      prev_x = this_x;
      prev_y = this_y;
    }
    current_color = (current_color + 1) % line_colors.size();
  }

  // write image
  string image = img.save(this->image_format);
  evbuffer_add(this->buf, image.data(), image.size());
}

void ImageRenderer::render_find_results(
    const unordered_map<string, FindResult>& data) const {
  throw runtime_error("find results can\'t be rendered as images");
}

void ImageRenderer::set_image_parameters(uint64_t width, uint64_t height,
    double y_min, double y_max) {
  this->width = width;
  this->height = height;
  this->y_min_fixed = y_min;
  this->y_max_fixed = y_max;
}

uint8_t ImageRenderer::background_color[3] = {255, 255, 255};

uint8_t ImageRenderer::foreground_color[3] = {0, 0, 0};

vector<vector<uint8_t>> ImageRenderer::line_colors({
  {200,   0,  50}, // red
  {  0, 200,   0}, // green
  {100, 100, 255}, // blue
  {200, 100, 255}, // purple
  {150, 100,  50}, // brown
  {255, 255,   0}, // yellow
  {  0, 150, 150}, // aqua
  {175, 175, 175}, // grey
  {255,   0, 255}, // magenta
  {255, 100, 100}, // pink
  {200, 200,   0}, // gold
  {200, 150, 200}, // rose

  // {  0,   0,   0}, // black
  // {255, 255, 255}, // white
  // {255, 165,   0}, // orange
  // {  0, 255, 255}, // cyan
  // {  0,   0, 255}, // darkblue
  // {  0, 255,   0}, // darkgreen
  // {255,   0,   0}, // darkred
  // {111, 111, 111}, // darkgrey
});
