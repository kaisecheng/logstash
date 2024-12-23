require_relative '../spec_helper'
require_relative '../../shared_examples/container_config'
require_relative '../../shared_examples/container_options'
require_relative '../../shared_examples/container'
require_relative '../../shared_examples/xpack'

describe 'A container running the full image' do
  it_behaves_like 'the container is configured correctly', 'full'
  it_behaves_like 'it runs with different configurations', 'full'
  it_behaves_like 'it applies settings correctly', 'full'
  it_behaves_like 'a container with xpack features', 'full'

  context 'The running container' do
    before do
      @image = find_image('full')
      @container = start_container(@image, {})
    end

    after do
      cleanup_container(@container)
    end

    it 'should be based on Red Hat Enterprise Linux' do
      expect(exec_in_container(@container, 'cat /etc/redhat-release')).to match /Red Hat Enterprise Linux/
    end
  end
end
